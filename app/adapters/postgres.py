"""Compatibility wrapper for the reference app's generic record table.

SDK consumers register their own tables with SQLAlchemyStore/TableBinding.
"""

from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Record
from app.db.session import SessionLocal
from sync_sdk.adapters.sqlalchemy import SQLAlchemyStore
from sync_sdk.gateway import SyncGateway
from sync_sdk.models import Mutation
from sync_sdk.registry import Entity, EntityRegistry


class _ReferenceRecordBinding:
    async def write(
        self, session: AsyncSession, mutation: Mutation, version: int
    ) -> None:
        record = await session.get(
            Record, {"entity": mutation.entity, "entity_id": mutation.entity_id}
        )
        if mutation.operation == "delete":
            if record is not None:
                await session.delete(record)
        elif record is not None:
            record.payload, record.version, record.updated_at = (
                mutation.payload,
                version,
                datetime.now(UTC),
            )
        else:
            session.add(
                Record(
                    entity=mutation.entity,
                    entity_id=mutation.entity_id,
                    payload=mutation.payload,
                    version=version,
                    updated_at=datetime.now(UTC),
                )
            )
        await session.flush()


class PostgresRemoteStore(SyncGateway):
    """Legacy name retained; the reference app explicitly opts in to 'task'."""

    def __init__(self, session_factory: Callable[[], AsyncSession] = SessionLocal):
        super().__init__(
            SQLAlchemyStore(session_factory, {"task": _ReferenceRecordBinding()}),
            EntityRegistry([Entity("task")]),
        )
