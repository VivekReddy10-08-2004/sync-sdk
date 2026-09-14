"""Reusable mutation pipeline with no database or web framework imports."""

import time

from pydantic import ValidationError

from .models import ChangeBatch, Mutation, MutationResult
from .ports import GatewayStore, GatewayTransaction, MutationRejected
from .registry import EntityRegistry
from .telemetry import PipelineTelemetry, safe_telemetry


class SyncGateway:
    def __init__(
        self,
        store: GatewayStore,
        entities: EntityRegistry,
        *,
        telemetry: PipelineTelemetry | None = None,
    ):
        self.store, self.entities = store, entities
        self.telemetry = safe_telemetry(telemetry)

    async def apply_mutations(self, mutations: list[Mutation]) -> list[MutationResult]:
        started = time.perf_counter()
        results: list[MutationResult] = []
        outcome = "ok"
        with self.telemetry.span(
            "sync.gateway.apply", {"sync.batch.size": len(mutations)}
        ) as span:
            try:
                async with self.store.transaction() as transaction:
                    for mutation in mutations:
                        result, is_new = await self._apply_mutation(
                            transaction, mutation
                        )
                        if is_new:
                            await transaction.save_result(mutation, result)
                        results.append(result)
            except Exception:
                outcome = "error"
                self.telemetry.count("sync.gateway.batch.failures")
                raise
            finally:
                self.telemetry.record(
                    "sync.gateway.batch.duration",
                    (time.perf_counter() - started) * 1000,
                    {"sync.outcome": outcome},
                )
                span.set_attribute("sync.outcome", outcome)

            for mutation, result in zip(mutations, results, strict=True):
                attributes = {
                    "sync.entity": mutation.entity,
                    "sync.operation": mutation.operation,
                    "sync.mutation.status": result.status,
                }
                self.telemetry.count("sync.gateway.mutations", attributes=attributes)
                span.add_event("sync.mutation.result", attributes)
                if result.status == "conflict":
                    self.telemetry.count(
                        "sync.gateway.conflicts",
                        attributes={"sync.entity": mutation.entity},
                    )
                elif result.status == "rejected":
                    self.telemetry.count(
                        "sync.gateway.failed_mutations",
                        attributes={
                            "sync.entity": mutation.entity,
                            "sync.mutation.status": result.status,
                        },
                    )
            return results

    async def _apply_mutation(
        self, transaction: GatewayTransaction, mutation: Mutation
    ) -> tuple[MutationResult, bool]:
        existing = await transaction.get_result(mutation.mutation_id)
        if existing is not None:
            return existing, False

        entity = self.entities.get(mutation.entity)
        rejection = None
        if entity is None:
            rejection = "Entity is not registered"
        else:
            try:
                mutation = entity.validate(mutation)
            except ValidationError:
                rejection = "Payload failed entity validation"

        if rejection:
            return (
                MutationResult(
                    mutation_id=mutation.mutation_id,
                    status="rejected",
                    message=rejection,
                ),
                True,
            )

        assert entity is not None
        version = await transaction.get_version(mutation.entity, mutation.entity_id)
        conflict = entity.conflict_policy(mutation, version)
        if conflict:
            return (
                MutationResult(
                    mutation_id=mutation.mutation_id,
                    status="conflict",
                    server_version=version,
                    message=conflict,
                ),
                True,
            )

        version += 1
        try:
            await transaction.write(mutation, version)
        except MutationRejected as error:
            return (
                MutationResult(
                    mutation_id=mutation.mutation_id,
                    status="rejected",
                    message=str(error),
                ),
                True,
            )

        await transaction.append_change(mutation, version)
        return (
            MutationResult(
                mutation_id=mutation.mutation_id,
                status="applied",
                server_version=version,
            ),
            True,
        )

    async def get_changes(self, cursor: int, limit: int = 100) -> ChangeBatch:
        if cursor < 0 or not 1 <= limit <= 1000:
            raise ValueError("Invalid cursor or page limit")

        started = time.perf_counter()
        with self.telemetry.span(
            "sync.gateway.pull", {"sync.cursor": cursor, "sync.page.limit": limit}
        ) as span:
            try:
                page = await self.store.get_changes(cursor, limit)
            except Exception:
                self.telemetry.count("sync.gateway.pull.failures")
                raise
            finally:
                self.telemetry.record(
                    "sync.gateway.pull.duration",
                    (time.perf_counter() - started) * 1000,
                )
            span.set_attribute("sync.changes.returned", len(page.changes))
            span.set_attribute("sync.has_more", page.has_more)
            self.telemetry.count("sync.gateway.changes.returned", len(page.changes))
            return page

    async def health_check(self) -> bool:
        return await self.store.health_check()
