"""Reusable mutation pipeline with no database or web framework imports."""

from pydantic import ValidationError

from .models import ChangeBatch, Mutation, MutationResult
from .ports import GatewayStore, MutationRejected
from .registry import EntityRegistry


class SyncGateway:
    def __init__(self, store: GatewayStore, entities: EntityRegistry):
        self.store, self.entities = store, entities

    async def apply_mutations(self, mutations: list[Mutation]) -> list[MutationResult]:
        results = []
        async with self.store.transaction() as tx:
            for mutation in mutations:
                existing = await tx.get_result(mutation.mutation_id)
                if existing is not None:
                    results.append(existing)
                    continue
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
                    result = MutationResult(
                        mutation_id=mutation.mutation_id,
                        status="rejected",
                        message=rejection,
                    )
                else:
                    assert entity is not None
                    version = await tx.get_version(mutation.entity, mutation.entity_id)
                    conflict = entity.conflict_policy(mutation, version)
                    if conflict:
                        result = MutationResult(
                            mutation_id=mutation.mutation_id,
                            status="conflict",
                            server_version=version,
                            message=conflict,
                        )
                    else:
                        version += 1
                        try:
                            await tx.write(mutation, version)
                        except MutationRejected as exc:
                            result = MutationResult(
                                mutation_id=mutation.mutation_id,
                                status="rejected",
                                message=str(exc),
                            )
                        else:
                            await tx.append_change(mutation, version)
                            result = MutationResult(
                                mutation_id=mutation.mutation_id,
                                status="applied",
                                server_version=version,
                            )
                await tx.save_result(mutation, result)
                results.append(result)
        return results

    async def get_changes(self, cursor: int, limit: int = 100) -> ChangeBatch:
        if cursor < 0 or not 1 <= limit <= 1000:
            raise ValueError("Invalid cursor or page limit")
        return await self.store.get_changes(cursor, limit)

    async def health_check(self) -> bool:
        return await self.store.health_check()
