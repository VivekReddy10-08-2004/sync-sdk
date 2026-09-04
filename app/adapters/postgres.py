from datetime import UTC, datetime
from typing import Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.models import (
    Change,
    ChangeBatch,
    Mutation,
    MutationResult,
)
from app.core.ports import RemoteStore
from app.db.models import ChangeLog, ProcessedMutation, Record
from app.db.session import SessionLocal


class PostgresRemoteStore(RemoteStore):
    def __init__(
        self,
        session_factory: Callable[[], AsyncSession] = SessionLocal,
    ):
        self.session_factory = session_factory

    async def apply_mutations(
        self,
        mutations: list[Mutation],
    ) -> list[MutationResult]:
        results: list[MutationResult] = []

        async with self.session_factory() as session:
            async with session.begin():
                for mutation in mutations:
                    existing_processed = await session.get(
                        ProcessedMutation,
                        str(mutation.mutation_id),
                    )

                    if existing_processed:
                        results.append(
                            MutationResult.model_validate(
                                existing_processed.result
                            )
                        )
                        continue

                    record = await session.get(
                        Record,
                        {
                            "entity": mutation.entity,
                            "entity_id": mutation.entity_id,
                        },
                    )

                    current_version = record.version if record else 0

                    if (
                        mutation.base_version is not None
                        and mutation.base_version != current_version
                    ):
                        result = MutationResult(
                            mutation_id=mutation.mutation_id,
                            status="conflict",
                            server_version=current_version,
                            message="Version mismatch",
                        )
                    else:
                        new_version = current_version + 1
                        now = datetime.now(UTC)

                        if mutation.operation == "delete":
                            if record:
                                await session.delete(record)
                        else:
                            if record:
                                record.payload = mutation.payload
                                record.version = new_version
                                record.updated_at = now
                            else:
                                session.add(
                                    Record(
                                        entity=mutation.entity,
                                        entity_id=mutation.entity_id,
                                        payload=mutation.payload,
                                        version=new_version,
                                        updated_at=now,
                                    )
                                )

                        session.add(
                            ChangeLog(
                                entity=mutation.entity,
                                entity_id=mutation.entity_id,
                                operation=mutation.operation,
                                payload=mutation.payload,
                                version=new_version,
                                created_at=now,
                            )
                        )

                        result = MutationResult(
                            mutation_id=mutation.mutation_id,
                            status="applied",
                            server_version=new_version,
                        )

                    session.add(
                        ProcessedMutation(
                            mutation_id=str(mutation.mutation_id),
                            client_id=mutation.client_id,
                            result=result.model_dump(mode="json"),
                            processed_at=datetime.now(UTC),
                        )
                    )

                    results.append(result)

        return results

    async def get_changes(
        self,
        cursor: int,
        limit: int = 100,
    ) -> ChangeBatch:
        async with self.session_factory() as session:
            stmt = (
                select(ChangeLog)
                .where(ChangeLog.sequence > cursor)
                .order_by(ChangeLog.sequence)
                .limit(limit + 1)
            )

            rows = list((await session.scalars(stmt)).all())

            has_more = len(rows) > limit
            rows = rows[:limit]

            changes = [
                Change(
                    sequence=row.sequence,
                    entity=row.entity,
                    entity_id=row.entity_id,
                    operation=row.operation,
                    payload=row.payload,
                    version=row.version,
                    created_at=row.created_at,
                )
                for row in rows
            ]

            new_cursor = changes[-1].sequence if changes else cursor

            return ChangeBatch(
                cursor=new_cursor,
                has_more=has_more,
                changes=changes,
            )

    async def health_check(self) -> bool:
        try:
            async with self.session_factory() as session:
                await session.execute(select(1))
            return True
        except Exception:
            return False