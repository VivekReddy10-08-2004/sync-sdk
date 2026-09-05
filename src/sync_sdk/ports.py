"""Database and framework independent contracts for sync integrations."""

from contextlib import AbstractAsyncContextManager
from typing import Protocol
from uuid import UUID

from .models import ChangeBatch, Mutation, MutationResult


class MutationRejected(Exception):
    """Adapter validation failed BEFORE any writes for this mutation.

    A binding may use this for invalid keys/payloads. It must never raise this
    after changing data; operational failures must abort the transaction instead.
    """


class RetryableError(Exception):
    """A transport can be retried without changing mutation IDs."""

    def __init__(self, message: str, *, retry_after: float = 0):
        super().__init__(message)
        self.retry_after = retry_after


class SyncTransport(Protocol):
    async def push(self, mutations: list[Mutation]) -> list[MutationResult]: ...
    async def pull(self, cursor: int, limit: int) -> ChangeBatch: ...


class LocalStore(Protocol):
    """Single-worker durable store; all mutating methods commit before returning.

    apply_page MUST atomically apply records/deletes and advance the cursor.
    acknowledge MUST atomically persist the whole batch of results. Pending work
    survives process interruption and retains immutable mutation IDs and bodies.
    """

    client_id: str

    @property
    def cursor(self) -> int: ...
    @property
    def retry_state(self) -> tuple[int, float]: ...
    def enqueue(self, mutation: Mutation) -> None: ...
    def pending(self, limit: int) -> list[Mutation]: ...
    def acknowledge(self, results: list[MutationResult]) -> None: ...
    def apply_page(self, page: ChangeBatch) -> None: ...
    def schedule_retry(self, attempts: int, retry_at: float) -> None: ...


class GatewayTransaction(Protocol):
    """One atomic, serialized gateway batch, including developer-owned data.

    The adapter must prevent concurrent duplicate IDs/version races. Versions
    survive deletion. Changes have strictly increasing, commit-ordered sequences:
    a later cursor must never hide a change that commits subsequently. Aborting
    or cancellation rolls back entity writes, versions, changes AND results.
    """

    async def get_result(self, mutation_id: UUID) -> MutationResult | None: ...
    async def get_version(self, entity: str, entity_id: str) -> int: ...
    async def write(self, mutation: Mutation, version: int) -> None:
        """Write business data and its sync version in this transaction."""
        ...

    async def append_change(self, mutation: Mutation, version: int) -> None: ...
    async def save_result(self, mutation: Mutation, result: MutationResult) -> None: ...


class GatewayStore(Protocol):
    """SQL/NoSQL adapter. No connection creation or schema creation in the engine."""

    def transaction(self) -> AbstractAsyncContextManager[GatewayTransaction]: ...
    async def get_changes(self, cursor: int, limit: int) -> ChangeBatch: ...
    async def health_check(self) -> bool: ...


class RemoteStore(Protocol):
    """Service boundary used by HTTP, direct in-process, or other integrations."""

    async def apply_mutations(
        self, mutations: list[Mutation]
    ) -> list[MutationResult]: ...
    async def get_changes(self, cursor: int, limit: int = 100) -> ChangeBatch: ...
    async def health_check(self) -> bool: ...
