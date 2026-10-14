"""MongoDB adapter for application collections and SDK sync metadata.

The adapter uses PyMongo's native asyncio API and multi-document transactions.
MongoDB must run as a replica set or sharded cluster. Registered payload fields
are written to application-owned collections; SDK state lives in prefixed
metadata collections in the same database.
"""

import asyncio
import time
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, create_model
from pymongo import ReturnDocument
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import CollectionInvalid, ConnectionFailure, PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from ..gateway import SyncGateway
from ..models import Change, ChangeBatch, Mutation, MutationResult
from ..ports import MutationRejected, RetryableError
from ..registry import ConflictPolicy, Entity, EntityRegistry, optimistic_version
from ..telemetry import PipelineTelemetry, safe_telemetry

Document = dict[str, Any]
_METADATA_SUFFIXES = ("clock", "versions", "processed_mutations", "change_log")


def _collection_names(prefix: str) -> dict[str, str]:
    if not prefix or "\0" in prefix or prefix.startswith("system."):
        raise ValueError("MongoDB metadata prefix is invalid")
    return {suffix: f"{prefix}{suffix}" for suffix in _METADATA_SUFFIXES}


async def _ensure_collection(database: AsyncDatabase, name: str) -> None:
    if name in await database.list_collection_names():
        return
    try:
        await database.create_collection(name)
    except CollectionInvalid:
        # Another initializer may have created it after the list operation.
        pass


async def initialize_mongodb_metadata(
    database: AsyncDatabase, *, metadata_prefix: str = "sync_"
) -> None:
    """Create SDK collections and the clock document before serving traffic."""
    names = _collection_names(metadata_prefix)
    for name in names.values():
        await _ensure_collection(database, name)
    await database[names["clock"]].update_one(
        {"_id": "clock"},
        {"$setOnInsert": {"sequence": 0, "lock_epoch": 0}},
        upsert=True,
    )


class MongoDBBinding(Protocol):
    """Custom MongoDB entity mapping; all operations must use the session."""

    collection_name: str

    async def write(
        self,
        database: AsyncDatabase,
        session: AsyncClientSession,
        mutation: Mutation,
        version: int,
    ) -> None: ...


class MongoCollectionBinding:
    """Map selected payload fields to top-level MongoDB document fields."""

    def __init__(
        self,
        collection_name: str,
        *,
        fields: Mapping[str, str],
        parse_key: Callable[[str], Any] = str,
        value_decoders: Mapping[str, Callable[[Any], Any]] | None = None,
    ):
        if (
            not collection_name
            or "\0" in collection_name
            or collection_name.startswith("system.")
        ):
            raise ValueError("MongoDB collection name is invalid")
        if not fields:
            raise ValueError("At least one MongoDB payload field is required")
        if len(set(fields.values())) != len(fields):
            raise ValueError("Payload fields must map to distinct document fields")
        if any(
            not name or name == "_id" or name.startswith("$") or "." in name
            for name in fields.values()
        ):
            raise ValueError(
                "Mapped MongoDB fields must be top-level fields other than _id"
            )
        overrides = dict(value_decoders or {})
        if not set(overrides).issubset(fields):
            raise ValueError("value_decoders must refer to registered payload fields")
        self.collection_name = collection_name
        self.fields = dict(fields)
        self.parse_key = parse_key
        self.value_decoders = overrides

    def encode_key(self, value: Any) -> str:
        encoded = str(value)
        self._decode_key(encoded)
        return encoded

    def _decode_key(self, entity_id: str) -> Any:
        try:
            value = self.parse_key(entity_id)
            if str(value) != entity_id:
                raise ValueError("Noncanonical key")
            return value
        except (ValueError, TypeError, OverflowError) as exc:
            raise MutationRejected(
                "Invalid entity ID; use the binding's encode_key()"
            ) from exc

    async def write(
        self,
        database: AsyncDatabase,
        session: AsyncClientSession,
        mutation: Mutation,
        version: int,
    ) -> None:
        del version
        key = self._decode_key(mutation.entity_id)
        collection = database[self.collection_name]
        if mutation.operation == "delete":
            await collection.delete_one({"_id": key}, session=session)
            return
        if set(mutation.payload) != set(self.fields):
            raise MutationRejected(
                "Payload must contain exactly the registered MongoDB fields"
            )
        try:
            values = {
                self.fields[field]: self.value_decoders.get(field, lambda item: item)(
                    value
                )
                for field, value in mutation.payload.items()
            }
        except (ValueError, TypeError, OverflowError) as exc:
            raise MutationRejected(
                "Payload value is incompatible with the registered MongoDB field"
            ) from exc
        await collection.update_one(
            {"_id": key}, {"$set": values}, upsert=True, session=session
        )


class MongoDBStore:
    """Transactional MongoDB implementation of the gateway storage contract."""

    def __init__(
        self,
        database: AsyncDatabase,
        bindings: Mapping[str, MongoDBBinding],
        *,
        metadata_prefix: str = "sync_",
        telemetry: PipelineTelemetry | None = None,
    ):
        self.database = database
        self.bindings = dict(bindings)
        self.names = _collection_names(metadata_prefix)
        self.telemetry = safe_telemetry(telemetry)
        self._writer_lock = asyncio.Lock()

    @staticmethod
    def _attributes(operation: str, outcome: str) -> dict[str, str]:
        return {
            "db.system.name": "mongodb",
            "sync.storage.operation": operation,
            "sync.outcome": outcome,
        }

    @asynccontextmanager
    async def transaction(self):
        started = time.perf_counter()
        outcome = "ok"
        with self.telemetry.span(
            "sync.storage.transaction", {"db.system.name": "mongodb"}
        ) as span:
            try:
                async with (
                    self._writer_lock,
                    self.database.client.start_session() as session,
                ):
                    transaction = await session.start_transaction(
                        read_concern=ReadConcern("snapshot"),
                        write_concern=WriteConcern("majority"),
                    )
                    async with transaction:
                        clock = await self.database[
                            self.names["clock"]
                        ].find_one_and_update(
                            {"_id": "clock"},
                            {"$inc": {"lock_epoch": 1}},
                            return_document=ReturnDocument.AFTER,
                            session=session,
                        )
                        if clock is None:
                            raise RuntimeError(
                                "MongoDB sync metadata is not initialized"
                            )
                        yield _MongoDBTransaction(
                            self.database,
                            self.bindings,
                            self.names,
                            session,
                            int(clock["sequence"]),
                        )
            except (ConnectionFailure, PyMongoError) as exc:
                outcome = "error"
                self.telemetry.count(
                    "sync.storage.failures",
                    attributes=self._attributes("transaction", outcome),
                )
                if (
                    isinstance(exc, ConnectionFailure)
                    or exc.has_error_label("TransientTransactionError")
                    or exc.has_error_label("UnknownTransactionCommitResult")
                ):
                    raise RetryableError(
                        str(exc), reason="storage", retry_after=0.1
                    ) from exc
                raise
            except BaseException:
                outcome = "error"
                self.telemetry.count(
                    "sync.storage.failures",
                    attributes=self._attributes("transaction", outcome),
                )
                raise
            finally:
                attributes = self._attributes("transaction", outcome)
                self.telemetry.count("sync.storage.transactions", attributes=attributes)
                self.telemetry.record(
                    "sync.storage.transaction.duration",
                    (time.perf_counter() - started) * 1000,
                    attributes,
                )
                span.set_attribute("sync.outcome", outcome)

    async def get_changes(self, cursor: int, limit: int) -> ChangeBatch:
        started = time.perf_counter()
        outcome = "ok"
        try:
            documents = await (
                self.database[self.names["change_log"]]
                .find({"_id": {"$gt": cursor}})
                .sort("_id", 1)
                .limit(limit + 1)
                .to_list(length=limit + 1)
            )
            page = [Change.model_validate(document) for document in documents[:limit]]
            return ChangeBatch(
                cursor=page[-1].sequence if page else cursor,
                has_more=len(documents) > limit,
                changes=page,
            )
        except BaseException:
            outcome = "error"
            self.telemetry.count(
                "sync.storage.failures",
                attributes=self._attributes("pull", outcome),
            )
            raise
        finally:
            self.telemetry.record(
                "sync.storage.pull.duration",
                (time.perf_counter() - started) * 1000,
                self._attributes("pull", outcome),
            )

    async def health_check(self) -> bool:
        started = time.perf_counter()
        outcome = "ok"
        try:
            response = await self.database.command("ping")
            clock = await self.database[self.names["clock"]].find_one(
                {"_id": "clock"}, projection={"_id": 1}
            )
            healthy = response.get("ok") == 1 and clock is not None
            if not healthy:
                outcome = "error"
            return healthy
        except (PyMongoError, OSError):
            outcome = "error"
            return False
        finally:
            self.telemetry.record(
                "sync.storage.health.duration",
                (time.perf_counter() - started) * 1000,
                self._attributes("health", outcome),
            )


class _MongoDBTransaction:
    def __init__(
        self,
        database: AsyncDatabase,
        bindings: Mapping[str, MongoDBBinding],
        names: Mapping[str, str],
        session: AsyncClientSession,
        sequence: int,
    ):
        self.database = database
        self.bindings = bindings
        self.names = names
        self.session = session
        self.sequence = sequence

    @staticmethod
    def _row_id(entity: str, entity_id: str) -> Document:
        return {"entity": entity, "entity_id": entity_id}

    async def get_result(self, mutation_id: UUID) -> MutationResult | None:
        document = await self.database[self.names["processed_mutations"]].find_one(
            {"_id": str(mutation_id)}, session=self.session
        )
        return (
            MutationResult.model_validate(document["result"])
            if document is not None
            else None
        )

    async def get_version(self, entity: str, entity_id: str) -> int:
        document = await self.database[self.names["versions"]].find_one(
            {"_id": self._row_id(entity, entity_id)}, session=self.session
        )
        return int(document["version"]) if document is not None else 0

    async def write(self, mutation: Mutation, version: int) -> None:
        binding = self.bindings.get(mutation.entity)
        if binding is None:
            raise ValueError(
                f"No MongoDB binding for registered entity: {mutation.entity}"
            )
        await binding.write(self.database, self.session, mutation, version)
        await self.database[self.names["versions"]].replace_one(
            {"_id": self._row_id(mutation.entity, mutation.entity_id)},
            {
                "_id": self._row_id(mutation.entity, mutation.entity_id),
                "version": version,
            },
            upsert=True,
            session=self.session,
        )

    async def append_change(self, mutation: Mutation, version: int) -> None:
        self.sequence += 1
        await self.database[self.names["change_log"]].insert_one(
            {
                "_id": self.sequence,
                "sequence": self.sequence,
                "entity": mutation.entity,
                "entity_id": mutation.entity_id,
                "operation": mutation.operation,
                "payload": mutation.payload,
                "version": version,
                "created_at": datetime.now(UTC),
            },
            session=self.session,
        )
        await self.database[self.names["clock"]].update_one(
            {"_id": "clock"},
            {"$set": {"sequence": self.sequence}},
            session=self.session,
        )

    async def save_result(self, mutation: Mutation, result: MutationResult) -> None:
        await self.database[self.names["processed_mutations"]].insert_one(
            {
                "_id": str(mutation.mutation_id),
                "client_id": mutation.client_id,
                "result": result.model_dump(mode="json"),
                "processed_at": datetime.now(UTC),
            },
            session=self.session,
        )


class MongoDBConfig:
    """Register MongoDB collections without performing database I/O."""

    def __init__(self, *, metadata_prefix: str = "sync_"):
        self.metadata_prefix = metadata_prefix
        self._metadata_names = set(_collection_names(metadata_prefix).values())
        self._bindings: dict[str, MongoCollectionBinding] = {}
        self._entities: dict[str, Entity] = {}

    def register_collection(
        self,
        name: str,
        collection_name: str,
        *,
        fields: Mapping[str, str] | None = None,
        schema: type[BaseModel] | None = None,
        parse_key: Callable[[str], Any] = str,
        value_decoders: Mapping[str, Callable[[Any], Any]] | None = None,
        conflict_policy: ConflictPolicy = optimistic_version,
    ) -> MongoCollectionBinding:
        if not name or len(name) > 191 or name in self._bindings:
            raise ValueError(
                "Entity names must be unique and contain 1 to 191 characters"
            )
        if collection_name in self._metadata_names:
            raise ValueError("Sync metadata collections cannot store business data")
        if any(
            binding.collection_name == collection_name
            for binding in self._bindings.values()
        ):
            raise ValueError(
                "Register a MongoDB collection once to preserve one version history"
            )
        if fields is None:
            if schema is None:
                raise ValueError("Provide fields or a schema for a MongoDB collection")
            fields = {field: field for field in schema.model_fields}
        binding = MongoCollectionBinding(
            collection_name,
            fields=fields,
            parse_key=parse_key,
            value_decoders=value_decoders,
        )
        if schema is None:
            definitions: dict[str, Any] = {field: (Any, ...) for field in fields}
            schema = create_model(
                f"{name}Payload",
                __config__=ConfigDict(extra="forbid"),
                **definitions,
            )
        self._bindings[name] = binding
        self._entities[name] = Entity(name, schema, conflict_policy)
        return binding

    def build(
        self,
        database: AsyncDatabase,
        *,
        telemetry: PipelineTelemetry | None = None,
    ) -> SyncGateway:
        return SyncGateway(
            MongoDBStore(
                database,
                self._bindings,
                metadata_prefix=self.metadata_prefix,
                telemetry=telemetry,
            ),
            EntityRegistry(self._entities.values()),
            telemetry=telemetry,
        )

    async def create_collections(self, database: AsyncDatabase) -> None:
        """Create registered collections, SDK collections, and the clock."""
        for binding in self._bindings.values():
            await _ensure_collection(database, binding.collection_name)
        await initialize_mongodb_metadata(
            database, metadata_prefix=self.metadata_prefix
        )
