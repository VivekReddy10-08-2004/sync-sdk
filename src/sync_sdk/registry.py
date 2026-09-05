"""Explicit opt-in entities and application-owned validation/conflict policy."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from pydantic import BaseModel

from .models import Mutation

ConflictPolicy = Callable[[Mutation, int], str | None]


def optimistic_version(mutation: Mutation, current_version: int) -> str | None:
    if mutation.base_version is not None and mutation.base_version != current_version:
        return "Version mismatch"
    return None


@dataclass(frozen=True)
class Entity:
    name: str
    schema: type[BaseModel] | None = None
    conflict_policy: ConflictPolicy = optimistic_version

    def validate(self, mutation: Mutation) -> Mutation:
        if self.schema is None or mutation.operation == "delete":
            return mutation
        payload = self.schema.model_validate(mutation.payload).model_dump(mode="json")
        return mutation.model_copy(update={"payload": payload})


class EntityRegistry:
    def __init__(self, entities: Iterable[Entity] = ()):
        self._entities: dict[str, Entity] = {}
        for entity in entities:
            self.register(entity)

    def register(self, entity: Entity) -> None:
        if not entity.name or entity.name in self._entities:
            raise ValueError("Entity names must be nonempty and unique")
        self._entities[entity.name] = entity

    def get(self, name: str) -> Entity | None:
        return self._entities.get(name)
