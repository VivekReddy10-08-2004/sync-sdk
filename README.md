# Sync SDK

A FastAPI sync gateway and a Python reliability client with a SQLite outbox.

```python
import asyncio
import httpx
from sync_sdk import SQLiteStore, SyncClient

async def main():
    with SQLiteStore("client.db", client_id="device-1") as store:
        async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=10) as http:
            sync = SyncClient(store, http, batch_size=100)
            mutation = sync.enqueue(
                entity="task", entity_id="task-1", operation="create",
                payload={"status": "open"}, base_version=0,
            )
            # enqueue commits to disk before returning, including while offline.
            report = await sync.sync_once()
            print(mutation.mutation_id, report, store.results())
            # For continuous reconnect/resume, await sync.run() until cancelled.

asyncio.run(main())
```

Reuse the database and client ID after restart. Call `enqueue` once per new user
operation: restarting synchronization does not require enqueueing it again.
`SQLiteStore.enqueue(Mutation(...))` also accepts caller-created stable IDs and
rejects reuse with different content.

- The outbox sends pending mutations in insertion order, preserving their IDs and
  base versions on every attempt. Acknowledged results are committed atomically.
  Applied, conflicting, and rejected results remain queryable with `results()`;
  conflicts and rejections are terminal and require application resolution using
  a new mutation ID. The SDK does not silently rebase versions.
- Transport failures, HTTP 408/429, and server errors schedule exponential backoff
  with jitter and a configurable cap. `Retry-After` can extend that delay. Attempt
  counts and wall-clock deadlines survive restarts. `sync_once()` returns a
  `retry_at` timestamp on failure or while waiting; `run()` waits and reconnects.
  Other HTTP errors and invalid protocol responses raise without discarding work.
- Each cycle pushes up to `max_batches` batches and pulls up to `max_batches`
  pages (10 each by default). Later cycles resume remaining work.
- Pulls apply server records, including deletes, and their cursor in one SQLite
  transaction. `get_record(entity, entity_id)` returns the last pulled payload and
  version. Pending edits are stored in the outbox; the record view reflects pulled
  server state and does not provide an optimistic UI overlay.
- SQLite uses WAL and FULL synchronous commits. If the server commits but the
  response is lost, replay uses the same mutation IDs and gateway idempotency.
  Cancellation propagates and leaves unacknowledged work pending.

Use one sync worker per database, on its owning thread, and one database per
client/remote dataset. Do not reuse it across gateways or accounts. Local SQLite
transactions are synchronous and bounded by page/batch size. The caller owns the
HTTP client (including authentication and timeouts) and store lifecycle. Result
history is retained without automatic pruning.

Run the gateway with `uv run uvicorn app.main:app --reload`. Run checks with
`uv run pytest -q` and `uv run ruff check src/sync_sdk tests/integration/test_reliability.py tests/conftest.py app/core/models.py`.

Reliability tests use the actual FastAPI routes and a fresh SQLAlchemy-backed
SQLite gateway database per test. Fault transports simulate offline connections,
server errors, lost committed responses, cancellation, and interrupted pulls.
A subprocess exits during a local page transaction to verify rollback and replay
from disk. These tests exercise process interruption; they do not simulate a
physical power failure or validate PostgreSQL-specific concurrency behavior.
