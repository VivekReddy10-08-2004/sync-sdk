"""The same gateway behavior must hold for SQL and non-SQL implementations."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Column, Integer, MetaData, String, Table, inspect, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from sync_sdk import (
    Change,
    ChangeBatch,
    Entity,
    EntityRegistry,
    Mutation,
    SQLiteStore,
    SyncClient,
    SyncGateway,
)
from sync_sdk.adapters.direct import DirectTransport
from sync_sdk.adapters.sqlalchemy import (
    SQLAlchemyStore,
    TableBinding,
    initialize_sync_metadata,
)
from sync_sdk.integrations.fastapi import create_sync_router


class TaskPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str


class DocumentStore:
    """Test-only transactional document adapter, NOT a durable NoSQL driver."""

    def __init__(self):
        self.state = {"documents": {}, "versions": {}, "results": {}, "changes": []}
        self.lock = asyncio.Lock()
        self.fail = False

    @asynccontextmanager
    async def transaction(self):
        async with self.lock:
            tx = DocumentTransaction(deepcopy(self.state), self.fail)
            yield tx
            self.state = tx.state

    async def get_changes(self, cursor, limit):
        rows = [c for c in self.state["changes"] if c.sequence > cursor]
        page = rows[:limit]
        return ChangeBatch(
            cursor=page[-1].sequence if page else cursor,
            has_more=len(rows) > limit,
            changes=page,
        )

    async def health_check(self):
        return True


class DocumentTransaction:
    def __init__(self, state, fail):
        self.state, self.fail = state, fail

    async def get_result(self, mutation_id):
        return self.state["results"].get(mutation_id)

    async def get_version(self, entity, entity_id):
        return self.state["versions"].get((entity, entity_id), 0)

    async def write(self, mutation, version):
        key = (mutation.entity, mutation.entity_id)
        self.state["versions"][key] = version
        if mutation.operation == "delete":
            self.state["documents"].pop(key, None)
        else:
            self.state["documents"][key] = mutation.payload
        if self.fail and mutation.entity_id == "2":
            raise RuntimeError("Simulated business write failure")

    async def append_change(self, mutation, version):
        self.state["changes"].append(
            Change(
                sequence=len(self.state["changes"]) + 1,
                entity=mutation.entity,
                entity_id=mutation.entity_id,
                operation=mutation.operation,
                payload=mutation.payload,
                version=version,
                created_at=datetime.now(UTC),
            )
        )

    async def save_result(self, mutation, result):
        self.state["results"][mutation.mutation_id] = result


@pytest.fixture(params=["sql", "document"])
async def backend(request, tmp_path):
    entities = EntityRegistry([Entity("tasks", TaskPayload)])
    if request.param == "document":
        store = DocumentStore()
        yield SyncGateway(store, entities), store, None
        return
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'business.db'}")
    metadata = MetaData()
    table = Table(
        "developer_tasks",
        metadata,
        Column("task_number", Integer, primary_key=True),
        Column("display_title", String(100), nullable=False),
        Column("private_note", String(100), server_default="internal"),
    )
    # The developer owns business DDL. SDK initialization must not create it.
    await initialize_sync_metadata(engine)
    async with engine.begin() as conn:
        names = await conn.run_sync(lambda c: inspect(c).get_table_names())
        assert "developer_tasks" not in names
        assert "records" not in names
        await conn.run_sync(metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = SQLAlchemyStore(
        sessions,
        {
            "tasks": TableBinding(
                table, fields={"title": "display_title"}, parse_key=int
            )
        },
    )
    try:
        yield SyncGateway(store, entities), store, table
    finally:
        await engine.dispose()


def mutation(
    entity_id="1", *, operation="create", base_version=0, payload=None, entity="tasks"
):
    return Mutation(
        mutation_id=uuid4(),
        client_id="client",
        entity=entity,
        entity_id=entity_id,
        operation=operation,
        base_version=base_version,
        payload={"title": "hello"} if payload is None else payload,
        created_at=datetime.now(UTC),
    )


async def test_adapter_contract(backend):
    gateway, store, table = backend
    create = mutation()
    first = await gateway.apply_mutations([create, create])
    assert first[0] == first[1]
    assert first[0].status == "applied"
    update = mutation(operation="update", base_version=1, payload={"title": "updated"})
    conflict = mutation(operation="update", base_version=1)
    results = await gateway.apply_mutations([update, conflict])
    assert [r.status for r in results] == ["applied", "conflict"]
    if table is not None:
        async with store.session_factory() as session:
            row = (await session.execute(select(table))).mappings().one()
            assert row["display_title"] == "updated"
            assert row["private_note"] == "internal"
    else:
        assert store.state["documents"][("tasks", "1")] == {"title": "updated"}
    first_page = await gateway.get_changes(0, 1)
    assert first_page.has_more
    second_page = await gateway.get_changes(first_page.cursor, 1)
    assert second_page.changes[0].version == 2
    assert not second_page.has_more
    delete = mutation(operation="delete", base_version=2, payload={})
    assert (await gateway.apply_mutations([delete]))[0].server_version == 3
    stale = mutation(base_version=0)
    assert (await gateway.apply_mutations([stale]))[0].status == "conflict"
    recreate = mutation(base_version=3)
    assert (await gateway.apply_mutations([recreate]))[0].server_version == 4
    assert await gateway.health_check()


async def test_registry_rejections_are_durable_and_do_not_write(backend):
    gateway, _, _ = backend
    bad = [
        mutation(entity="private_table"),
        mutation(payload={"title": "x", "private_note": "leak"}),
        mutation(payload={}),
    ]
    results = await gateway.apply_mutations(bad)
    assert [r.status for r in results] == ["rejected"] * 3
    assert await gateway.apply_mutations(bad) == results
    assert not (await gateway.get_changes(0)).changes


async def test_custom_conflict_policy(backend):
    gateway, _, _ = backend
    gateway.entities = EntityRegistry(
        [Entity("tasks", TaskPayload, conflict_policy=lambda m, v: None)]
    )
    assert (await gateway.apply_mutations([mutation(base_version=99)]))[
        0
    ].status == "applied"


async def test_concurrent_duplicates_and_stale_versions(backend):
    gateway, _, _ = backend
    create = mutation()
    a, b = await asyncio.gather(
        gateway.apply_mutations([create]), gateway.apply_mutations([create])
    )
    assert a == b
    a, b = await asyncio.gather(
        gateway.apply_mutations([mutation(operation="update", base_version=1)]),
        gateway.apply_mutations([mutation(operation="update", base_version=1)]),
    )
    assert sorted([a[0].status, b[0].status]) == ["applied", "conflict"]
    assert len((await gateway.get_changes(0)).changes) == 2


async def test_batch_failure_rolls_back_business_data_and_all_metadata(backend):
    gateway, store, table = backend
    batch = [mutation(), mutation("2")]
    if table is None:
        store.fail = True
    else:
        original = store.bindings["tasks"]

        class FailingBinding:
            async def write(self, session, mutation, version):
                await original.write(session, mutation, version)
                if mutation.entity_id == "2":
                    raise RuntimeError("Simulated business write failure")

        store.bindings["tasks"] = FailingBinding()
    with pytest.raises(RuntimeError, match="business write"):
        await gateway.apply_mutations(batch)
    assert not (await gateway.get_changes(0)).changes
    if table is None:
        assert not store.state["documents"]
        store.fail = False
    else:
        async with store.session_factory() as session:
            assert not (await session.execute(select(table))).all()
        store.bindings["tasks"] = original
    results = await gateway.apply_mutations(batch)
    assert [r.server_version for r in results] == [1, 1]
    assert [c.sequence for c in (await gateway.get_changes(0)).changes] == [1, 2]


async def test_embedded_and_http_integrations_share_pipeline(backend, tmp_path):
    gateway, _, _ = backend
    application = FastAPI()
    application.include_router(create_sync_router(lambda: gateway, prefix="/app/sync"))
    with SQLiteStore(tmp_path / "local.db", client_id="client") as local:
        sdk = SyncClient(local, transport=DirectTransport(gateway))
        sdk.enqueue(
            entity="tasks",
            entity_id="1",
            operation="create",
            payload={"title": "embedded"},
        )
        assert (await sdk.sync_once()).pushed == 1
        assert local.get_record("tasks", "1")["payload"] == {"title": "embedded"}
        async with AsyncClient(
            transport=ASGITransport(app=application), base_url="http://test"
        ) as http:
            response = await http.get("/app/sync/changes")
            assert response.status_code == 200
            assert response.json()["changes"][0]["payload"] == {"title": "embedded"}
            response = await http.post(
                "/app/sync/mutations", json=[mutation("2").model_dump(mode="json")]
            )
            assert response.json()[0]["status"] == "applied"
        assert (await sdk.sync_once()).pulled == 1


async def test_table_binding_rejects_alias_ids_before_writing(backend):
    gateway, _, table = backend
    if table is None:
        pytest.skip("Scalar-key validation is specific to the SQL table binding")
    results = await gateway.apply_mutations(
        [mutation("01"), mutation("invalid"), mutation("1")]
    )
    assert [r.status for r in results] == ["rejected", "rejected", "applied"]
    assert len((await gateway.get_changes(0)).changes) == 1


async def test_sql_cancellation_rolls_back_inflight_business_write(backend):
    gateway, store, table = backend
    if table is None:
        pytest.skip("This cancellation test pauses an actual SQL write")
    entered = asyncio.Event()
    original = store.bindings["tasks"]

    class PausedBinding:
        async def write(self, session, mutation, version):
            await original.write(session, mutation, version)
            entered.set()
            await asyncio.Event().wait()

    store.bindings["tasks"] = PausedBinding()
    item = mutation()
    worker = asyncio.create_task(gateway.apply_mutations([item]))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
    assert not (await gateway.get_changes(0)).changes
    async with store.session_factory() as session:
        assert not (await session.execute(select(table))).all()
    store.bindings["tasks"] = original
    assert (await gateway.apply_mutations([item]))[0].server_version == 1
