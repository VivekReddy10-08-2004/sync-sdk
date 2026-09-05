"""Application configuration for SQL schemas and their sync registrations."""

from collections.abc import Callable, Mapping
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, create_model
from sqlalchemy import JSON, Connection, MetaData, Table
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from ..gateway import SyncGateway
from ..registry import ConflictPolicy, Entity, EntityRegistry, optimistic_version
from .sqlalchemy import (
    SQLAlchemyStore,
    TableBinding,
    initialize_sync_metadata,
    sync_metadata,
)


class SQLAlchemyConfig:
    """Declare participating tables once; create them only when explicitly asked.

    Native SQLAlchemy tables retain application-defined columns, indexes, foreign
    keys, constraints, and defaults. Registration and build() perform no I/O.
    """

    def __init__(self) -> None:
        self._bindings: dict[str, TableBinding] = {}
        self._entities: dict[str, Entity] = {}

    def register_table(
        self,
        name: str,
        table: Table,
        *,
        fields: Mapping[str, str] | None = None,
        schema: type[BaseModel] | None = None,
        key_parsers: Mapping[str, Callable[[str], Any]] | None = None,
        value_decoders: Mapping[str, Callable[[Any], Any]] | None = None,
        conflict_policy: ConflictPolicy = optimistic_version,
    ) -> TableBinding:
        """Register a new or existing table and return its key encoder.

        Omit fields to sync every non-primary-key column, or explicitly select a
        subset. schema is application-owned validation (including defaults).
        """
        if not name or len(name) > 191 or name in self._bindings:
            raise ValueError(
                "Entity names must be unique and contain 1 to 191 characters"
            )
        if table.name in sync_metadata.tables:
            raise ValueError(
                "Sync metadata tables cannot be registered as business data"
            )
        if any(
            binding.table.fullname == table.fullname
            for binding in self._bindings.values()
        ):
            raise ValueError(
                "Register a physical table once to preserve one version history per row"
            )
        if fields is None:
            fields = {
                column.name: column.name
                for column in table.columns
                if not column.primary_key
            }
        binding = TableBinding(
            table, fields=fields, key_parsers=key_parsers, value_decoders=value_decoders
        )
        if schema is None:
            schema = self._payload_schema(name, table, fields)
        self._bindings[name] = binding
        self._entities[name] = Entity(name, schema, conflict_policy)
        return binding

    @staticmethod
    def _payload_schema(
        name: str, table: Table, fields: Mapping[str, str]
    ) -> type[BaseModel]:
        definitions: dict[str, Any] = {}
        for field, column_name in fields.items():
            column = table.c[column_name]
            annotation: Any = (
                Any if isinstance(column.type, JSON) else column.type.python_type
            )
            default: Any = ...
            if column.nullable:
                annotation = annotation | None
            if column.default is not None and column.default.is_scalar:
                default = cast(Any, column.default).arg
            definitions[field] = (annotation, default)
        return create_model(
            f"{name}Payload", __config__=ConfigDict(extra="forbid"), **definitions
        )

    def build(self, session_factory: Callable[[], AsyncSession]) -> SyncGateway:
        """Snapshot this configuration into an embeddable gateway."""
        return SyncGateway(
            SQLAlchemyStore(session_factory, self._bindings),
            EntityRegistry(self._entities.values()),
        )

    async def create_tables(self, engine: AsyncEngine) -> None:
        """Explicit setup: create registered business tables and sync metadata.

        Existing tables are left intact. This is not schema migration, ALTER,
        backfill, or change capture. Use application migrations for schema changes.
        Foreign-key prerequisites must be registered or already exist. Run setup
        once before serving traffic, rather than from concurrent workers.
        """
        tables = [binding.table for binding in self._bindings.values()]
        async with engine.begin() as connection:
            await connection.run_sync(self._create_tables, tables)
        await initialize_sync_metadata(engine)

    @staticmethod
    def _create_tables(connection: Connection, tables: list[Table]) -> None:
        # Native create_all handles dependency ordering and deferred ALTER FKs.
        # Its explicit tables argument prevents creation of unregistered tables.
        metadata = tables[0].metadata if tables else MetaData()
        metadata.create_all(connection, tables=tables, checkfirst=True)
