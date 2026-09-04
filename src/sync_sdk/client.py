"""Bounded sync cycles and an optional reconnect loop."""

import asyncio
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Literal
from uuid import uuid4

import httpx
from pydantic import TypeAdapter

from .models import ChangeBatch, Mutation, MutationResult
from .store import SQLiteStore


@dataclass(frozen=True)
class SyncReport:
    pushed: int = 0
    pulled: int = 0
    retry_at: float | None = None


class SyncClient:
    def __init__(
        self,
        store: SQLiteStore,
        http: httpx.AsyncClient,
        *,
        batch_size: int = 100,
        max_batches: int = 10,
        backoff_base: float = 1,
        backoff_cap: float = 60,
        clock: Callable[[], float] = time.time,
        jitter: Callable[[], float] = random.random,
    ):
        if not 1 <= batch_size <= 1000 or max_batches < 1:
            raise ValueError("Invalid batch limits")
        if backoff_base <= 0 or backoff_cap < backoff_base:
            raise ValueError("Invalid backoff limits")
        self.store, self.http = store, http
        self.batch_size, self.max_batches = batch_size, max_batches
        self.backoff_base, self.backoff_cap = backoff_base, backoff_cap
        self.clock, self.jitter = clock, jitter
        self._lock = asyncio.Lock()

    def enqueue(
        self,
        *,
        entity: str,
        entity_id: str,
        operation: Literal["create", "update", "delete"],
        payload: dict | None = None,
        base_version: int | None = None,
    ) -> Mutation:
        mutation = Mutation(
            mutation_id=uuid4(),
            client_id=self.store.client_id,
            entity=entity,
            entity_id=entity_id,
            operation=operation,
            payload=payload or {},
            base_version=base_version,
            created_at=datetime.now(UTC),
        )
        self.store.enqueue(mutation)
        return mutation

    def _retry_after(self, response: httpx.Response) -> float:
        value = response.headers.get("Retry-After", "")
        try:
            return max(0, float(value))
        except ValueError:
            try:
                return max(0, parsedate_to_datetime(value).timestamp() - self.clock())
            except (ValueError, TypeError, OverflowError):
                return 0

    async def sync_once(self) -> SyncReport:
        """Push and pull bounded batches; transient failures return a persisted deadline.

        Cancellation propagates. Unknown outcomes remain pending for idempotent replay.
        Protocol errors and permanent HTTP failures propagate without dropping work.
        """
        async with self._lock:
            attempts, retry_at = self.store.retry_state
            if retry_at > self.clock():
                return SyncReport(retry_at=retry_at)
            pushed = pulled = 0
            retry_after = 0.0
            try:
                for _ in range(self.max_batches):
                    batch = self.store.pending(self.batch_size)
                    if not batch:
                        break
                    response = await self.http.post(
                        "/v1/sync/mutations",
                        json=[m.model_dump(mode="json") for m in batch],
                    )
                    retry_after = self._retry_after(response)
                    response.raise_for_status()
                    results = TypeAdapter(list[MutationResult]).validate_python(
                        response.json()
                    )
                    if len(results) != len(batch) or {
                        r.mutation_id for r in results
                    } != {m.mutation_id for m in batch}:
                        raise ValueError(
                            "Mutation acknowledgements do not match submitted batch"
                        )
                    self.store.acknowledge(results)
                    pushed += len(results)
                for _ in range(self.max_batches):
                    response = await self.http.get(
                        "/v1/sync/changes",
                        params={"cursor": self.store.cursor, "limit": self.batch_size},
                    )
                    retry_after = self._retry_after(response)
                    response.raise_for_status()
                    page = ChangeBatch.model_validate(response.json())
                    self.store.apply_page(page)
                    pulled += len(page.changes)
                    if not page.has_more:
                        break
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                if (
                    isinstance(exc, httpx.HTTPStatusError)
                    and exc.response.status_code not in (408, 429)
                    and exc.response.status_code < 500
                ):
                    raise
                ceiling = min(
                    self.backoff_cap, self.backoff_base * 2 ** min(attempts, 30)
                )
                delay = max(retry_after, ceiling * (0.5 + 0.5 * self.jitter()))
                retry_at = self.clock() + delay
                self.store.schedule_retry(attempts + 1, retry_at)
                return SyncReport(pushed, pulled, retry_at)
            self.store.schedule_retry(0, 0)
            return SyncReport(pushed, pulled)

    async def run(self, *, poll_interval: float = 1) -> None:
        """Reconnect until cancelled; the caller owns HTTP and SQLite lifetimes."""
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        while True:
            report = await self.sync_once()
            delay = (
                max(0, report.retry_at - self.clock())
                if report.retry_at is not None
                else poll_interval
            )
            await asyncio.sleep(delay)
