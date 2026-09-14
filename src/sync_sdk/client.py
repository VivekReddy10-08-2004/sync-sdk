"""Bounded sync cycles and an optional reconnect loop."""

import asyncio
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from .models import Mutation
from .ports import LocalStore, RetryableError, SyncTransport
from .telemetry import PipelineTelemetry, safe_telemetry

if TYPE_CHECKING:
    import httpx


@dataclass(frozen=True)
class SyncReport:
    pushed: int = 0
    pulled: int = 0
    retry_at: float | None = None


class SyncClient:
    def __init__(
        self,
        store: LocalStore,
        http: "httpx.AsyncClient | None" = None,
        *,
        transport: SyncTransport | None = None,
        batch_size: int = 100,
        max_batches: int = 10,
        backoff_base: float = 1,
        backoff_cap: float = 60,
        clock: Callable[[], float] = time.time,
        jitter: Callable[[], float] = random.random,
        telemetry: PipelineTelemetry | None = None,
    ):
        if not 1 <= batch_size <= 1000 or max_batches < 1:
            raise ValueError("Invalid batch limits")
        if backoff_base <= 0 or backoff_cap < backoff_base:
            raise ValueError("Invalid backoff limits")
        if (http is None) == (transport is None):
            raise ValueError("Provide exactly one of transport or http")
        if http is not None:
            from .adapters.http import HTTPTransport

            transport = HTTPTransport(http, clock=clock)
        assert transport is not None
        self.store, self.transport = store, transport
        self.batch_size, self.max_batches = batch_size, max_batches
        self.backoff_base, self.backoff_cap = backoff_base, backoff_cap
        self.clock, self.jitter = clock, jitter
        self.telemetry = safe_telemetry(telemetry)
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
        self.telemetry.gauge("sync.client.queue.depth", self.store.pending_count())
        return mutation

    async def sync_once(self) -> SyncReport:
        """Push and pull bounded batches; save retry time after connection failures."""
        async with self._lock:
            started = time.perf_counter()
            queue_depth = self.store.pending_count()
            self.telemetry.gauge("sync.client.queue.depth", queue_depth)
            with self.telemetry.span(
                "sync.client.sync", {"sync.queue.depth": queue_depth}
            ) as sync_span:
                attempts, retry_at = self.store.retry_state
                pushed = pulled = 0
                outcome = "ok"
                try:
                    if retry_at > self.clock():
                        outcome = "waiting"
                        sync_span.set_attribute("sync.retry.at", retry_at)
                        return SyncReport(retry_at=retry_at)

                    for _ in range(self.max_batches):
                        batch = self.store.pending(self.batch_size)
                        if not batch:
                            break
                        with self.telemetry.span(
                            "sync.client.push", {"sync.batch.size": len(batch)}
                        ):
                            results = await self.transport.push(batch)
                        if len(results) != len(batch) or {
                            result.mutation_id for result in results
                        } != {mutation.mutation_id for mutation in batch}:
                            raise ValueError(
                                "Mutation acknowledgements do not match submitted batch"
                            )
                        self.store.acknowledge(results)
                        pushed += len(results)
                        for result in results:
                            self.telemetry.count(
                                "sync.client.mutations",
                                attributes={"sync.mutation.status": result.status},
                            )

                    for _ in range(self.max_batches):
                        with self.telemetry.span(
                            "sync.client.pull", {"sync.cursor": self.store.cursor}
                        ):
                            page = await self.transport.pull(
                                self.store.cursor, self.batch_size
                            )
                        self.store.apply_page(page)
                        pulled += len(page.changes)
                        self.telemetry.count(
                            "sync.client.changes.pulled", len(page.changes)
                        )
                        if not page.has_more:
                            break
                except RetryableError as error:
                    outcome = "retry"
                    ceiling = min(
                        self.backoff_cap, self.backoff_base * 2 ** min(attempts, 30)
                    )
                    delay = max(
                        error.retry_after, ceiling * (0.5 + 0.5 * self.jitter())
                    )
                    retry_at = self.clock() + delay
                    self.store.schedule_retry(attempts + 1, retry_at)
                    failure_attributes = {"sync.failure.type": error.reason}
                    self.telemetry.count(
                        "sync.client.transport.failures",
                        attributes=failure_attributes,
                    )
                    if error.reason == "connection":
                        self.telemetry.count("sync.client.connection.failures")
                    self.telemetry.count(
                        "sync.client.retries", attributes=failure_attributes
                    )
                    self.telemetry.record("sync.client.retry.delay", delay)
                    sync_span.add_event(
                        "sync.retry.scheduled",
                        {
                            "sync.retry.attempt": attempts + 1,
                            "sync.retry.delay": delay,
                            "sync.failure.type": error.reason,
                        },
                    )
                    return SyncReport(pushed, pulled, retry_at)
                except Exception:
                    outcome = "error"
                    self.telemetry.count("sync.client.failures")
                    raise
                else:
                    self.store.schedule_retry(0, 0)
                    return SyncReport(pushed, pulled)
                finally:
                    depth = self.store.pending_count()
                    self.telemetry.gauge("sync.client.queue.depth", depth)
                    self.telemetry.record(
                        "sync.client.sync.duration",
                        (time.perf_counter() - started) * 1000,
                        {"sync.outcome": outcome},
                    )
                    sync_span.set_attribute("sync.outcome", outcome)
                    sync_span.set_attribute("sync.mutations.pushed", pushed)
                    sync_span.set_attribute("sync.changes.pulled", pulled)
                    sync_span.set_attribute("sync.queue.depth", depth)

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
