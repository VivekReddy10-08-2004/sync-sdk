import asyncio
import os
import subprocess
import sys

import httpx
import pytest

from sync_sdk import ChangeBatch, SQLiteStore, SyncClient

pytestmark = pytest.mark.asyncio


class FaultTransport(httpx.AsyncBaseTransport):
    """Forward to the actual gateway, optionally lose the committed response."""

    def __init__(
        self, gateway, *, failures=0, after_commit=False, status=None, cancel=False
    ):
        self.gateway = gateway
        self.failures = failures
        self.after_commit = after_commit
        self.status = status
        self.cancel = cancel
        self.requests = []

    async def handle_async_request(self, request):
        self.requests.append(request)
        if self.failures and not self.after_commit:
            self.failures -= 1
            if self.status:
                return httpx.Response(
                    self.status, headers={"Retry-After": "5"}, request=request
                )
            raise httpx.ConnectError("offline", request=request)
        response = await self.gateway.request(
            request.method,
            str(request.url),
            content=request.content,
            headers=request.headers,
        )
        assert response.status_code == 200, response.text
        if self.failures:
            self.failures -= 1
            if self.cancel:
                raise asyncio.CancelledError()
            raise httpx.ReadTimeout("response lost after commit", request=request)
        return response


def enqueue(sdk, entity_id="one", **kwargs):
    return sdk.enqueue(
        entity="task",
        entity_id=entity_id,
        operation=kwargs.pop("operation", "create"),
        **kwargs,
    )


async def test_offline_restart_retry_and_bounded_batches(client, tmp_path):
    path = tmp_path / "local.db"
    now = [100.0]
    transport = FaultTransport(client, failures=1)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        with SQLiteStore(path, client_id="a") as store:
            sdk = SyncClient(
                store,
                http,
                batch_size=2,
                max_batches=1,
                clock=lambda: now[0],
                jitter=lambda: 1,
            )
            ids = [enqueue(sdk, str(i)).mutation_id for i in range(5)]
            assert (await sdk.sync_once()).retry_at == 101
            assert len(store.pending(100)) == 5
        with SQLiteStore(path, client_id="a") as store:
            sdk = SyncClient(
                store,
                http,
                batch_size=2,
                max_batches=1,
                clock=lambda: now[0],
                jitter=lambda: 1,
            )
            assert (await sdk.sync_once()).retry_at == 101
            assert len(transport.requests) == 1
            now[0] = 101
            first = await sdk.sync_once()
            assert (first.pushed, first.pulled) == (2, 2)
            await sdk.sync_once()
            await sdk.sync_once()
            assert not store.pending(100)
            assert [r.mutation_id for r in store.results()] == ids
            assert store.retry_state == (0, 0)
            assert store.get_record("task", "4")["version"] == 1
            checkpoint = store.cursor
        with SQLiteStore(path, client_id="a") as store:
            assert store.cursor == checkpoint
            assert (await SyncClient(store, http).sync_once()).pulled == 0


@pytest.mark.parametrize("cancel", [False, True])
async def test_lost_acknowledgement_replays_idempotently(client, tmp_path, cancel):
    path = tmp_path / "local.db"
    transport = FaultTransport(client, failures=1, after_commit=True, cancel=cancel)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        with SQLiteStore(path, client_id="a") as store:
            sdk = SyncClient(store, http, clock=lambda: 0)
            mutation = enqueue(sdk)
            if cancel:
                with pytest.raises(asyncio.CancelledError):
                    await sdk.sync_once()
            else:
                assert (await sdk.sync_once()).retry_at is not None
            assert store.pending(10)[0].mutation_id == mutation.mutation_id
        with SQLiteStore(path, client_id="a") as store:
            await SyncClient(store, http, clock=lambda: 100).sync_once()
            assert not store.pending(10)
            assert store.results()[0].server_version == 1
        changes = (await client.get("/v1/sync/changes")).json()["changes"]
        assert len(changes) == 1


@pytest.mark.parametrize("status", [408, 429, 503])
async def test_backoff_persists_and_respects_retry_after(client, tmp_path, status):
    now = [100.0]
    transport = FaultTransport(client, failures=4, status=status)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        with SQLiteStore(tmp_path / "local.db", client_id="a") as store:
            sdk = SyncClient(
                store,
                http,
                backoff_base=2,
                backoff_cap=8,
                clock=lambda: now[0],
                jitter=lambda: 1,
            )
            enqueue(sdk)
            for attempt, delay in enumerate([5, 5, 8, 8], 1):
                report = await sdk.sync_once()
                assert report.retry_at == now[0] + delay
                assert store.retry_state == (attempt, report.retry_at)
                now[0] = report.retry_at
            await sdk.sync_once()
            assert store.retry_state == (0, 0)


async def test_permanent_http_failure_keeps_mutation(client, tmp_path):
    transport = FaultTransport(client, failures=1, status=401)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        with SQLiteStore(tmp_path / "local.db", client_id="a") as store:
            sdk = SyncClient(store, http)
            enqueue(sdk)
            with pytest.raises(httpx.HTTPStatusError):
                await sdk.sync_once()
            assert len(store.pending(10)) == 1
            assert store.retry_state == (0, 0)


async def test_conflict_retained_and_delete_propagates(client, tmp_path):
    with SQLiteStore(tmp_path / "local.db", client_id="a") as store:
        sdk = SyncClient(store, client)
        enqueue(sdk, payload={"done": False})
        await sdk.sync_once()
        enqueue(sdk, operation="update", base_version=999)
        enqueue(sdk, "other")
        report = await sdk.sync_once()
        assert report.pushed == 2
        assert [r.status for r in store.results()] == ["applied", "conflict", "applied"]
        assert not store.pending(10)
        enqueue(sdk, operation="delete", base_version=1)
        await sdk.sync_once()
        assert store.get_record("task", "one") is None
        assert store.get_record("task", "other") is not None


async def test_pull_failure_resumes_committed_page(client, tmp_path):
    with SQLiteStore(tmp_path / "writer.db", client_id="writer") as writer:
        sdk = SyncClient(writer, client)
        for i in range(3):
            enqueue(sdk, str(i))
        await sdk.sync_once()
    cursors = []

    async def handle(request):
        cursors.append(int(request.url.params["cursor"]))
        if len(cursors) == 2:
            raise httpx.ReadTimeout("pull interrupted", request=request)
        return await client.get(str(request.url))

    path = tmp_path / "reader.db"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), base_url="http://test"
    ) as http:
        with SQLiteStore(path, client_id="reader") as store:
            report = await SyncClient(
                store, http, batch_size=1, clock=lambda: 0
            ).sync_once()
            assert report.pulled == 1
            checkpoint = store.cursor
        with SQLiteStore(path, client_id="reader") as store:
            report = await SyncClient(
                store, http, batch_size=1, clock=lambda: 100
            ).sync_once()
            assert report.pulled == 2
            assert cursors[1] == cursors[2] == checkpoint


async def test_process_crash_rolls_back_records_and_checkpoint(client, tmp_path):
    path = tmp_path / "local.db"
    with SQLiteStore(path, client_id="a") as store:
        sdk = SyncClient(store, client)
        mutation = enqueue(sdk)
        await client.post("/v1/sync/mutations", json=[mutation.model_dump(mode="json")])
    page = (await client.get("/v1/sync/changes")).json()
    page_path = tmp_path / "page.json"
    page_path.write_text(ChangeBatch.model_validate(page).model_dump_json())
    script = """
import os, sys
from sync_sdk import SQLiteStore, ChangeBatch
store = SQLiteStore(sys.argv[1], client_id='a')
def crash(key, value):
    os._exit(23)
store._set = crash
store.apply_page(ChangeBatch.model_validate_json(open(sys.argv[2]).read()))
"""
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", script, str(path), str(page_path)],
        env=os.environ.copy(),
        timeout=10,
        check=False,
    )
    assert result.returncode == 23
    with SQLiteStore(path, client_id="a") as store:
        assert store.cursor == 0
        assert store.get_record("task", "one") is None
        assert len(store.pending(10)) == 1
        await SyncClient(store, client).sync_once()
        assert store.cursor == page["cursor"]
        assert store.get_record("task", "one")["version"] == 1
        assert len((await client.get("/v1/sync/changes")).json()["changes"]) == 1


async def test_bad_ack_does_not_drop_outbox(client, tmp_path):
    async def handle(request):
        await client.post(
            str(request.url), content=request.content, headers=request.headers
        )
        return httpx.Response(200, json=[])

    with SQLiteStore(tmp_path / "local.db", client_id="a") as store:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handle), base_url="http://test"
        ) as http:
            sdk = SyncClient(store, http)
            enqueue(sdk)
            with pytest.raises(ValueError, match="acknowledgements"):
                await sdk.sync_once()
            assert len(store.pending(10)) == 1
        await SyncClient(store, client).sync_once()
        assert store.results()[0].server_version == 1


async def test_reconnect_loop_recovers_and_cancels_cleanly(client, tmp_path):
    transport = FaultTransport(client, failures=1)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        with SQLiteStore(tmp_path / "local.db", client_id="a") as store:
            sdk = SyncClient(store, http, backoff_base=0.01, backoff_cap=0.01)
            enqueue(sdk)
            worker = asyncio.create_task(sdk.run(poll_interval=0.01))
            try:
                async with asyncio.timeout(5):
                    while store.get_record("task", "one") is None:
                        await asyncio.sleep(0.01)
            finally:
                worker.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await worker
            assert len(store.results()) == 1
            assert not store.pending(10)


async def test_invalid_page_preserves_checkpoint(client, tmp_path):
    with SQLiteStore(tmp_path / "local.db", client_id="a") as store:
        mutation = enqueue(SyncClient(store, client))
        await client.post("/v1/sync/mutations", json=[mutation.model_dump(mode="json")])
        page = ChangeBatch.model_validate((await client.get("/v1/sync/changes")).json())
        page.cursor += 1
        with pytest.raises(ValueError, match="checkpoint"):
            store.apply_page(page)
        assert store.cursor == 0
        assert store.get_record("task", "one") is None


async def test_database_identity_and_immutable_mutation_ids(tmp_path):
    path = tmp_path / "local.db"
    async with httpx.AsyncClient() as http:
        with SQLiteStore(path, client_id="a") as store:
            mutation = enqueue(SyncClient(store, http))
            store.enqueue(mutation)
            assert len(store.pending(10)) == 1
            mutation.payload = {"changed": True}
            with pytest.raises(ValueError, match="reuse"):
                store.enqueue(mutation)
    with pytest.raises(ValueError, match="different client_id"):
        SQLiteStore(path, client_id="b")


async def test_dependent_mutations_keep_fifo_order_in_batch(client, tmp_path):
    with SQLiteStore(tmp_path / "local.db", client_id="a") as store:
        sdk = SyncClient(store, client)
        enqueue(sdk, base_version=0)
        enqueue(sdk, operation="update", base_version=1, payload={"done": True})
        enqueue(sdk, operation="delete", base_version=2)
        report = await sdk.sync_once()
        assert (report.pushed, report.pulled) == (3, 3)
        assert [r.server_version for r in store.results()] == [1, 2, 3]
        assert all(r.status == "applied" for r in store.results())
        assert store.get_record("task", "one") is None
