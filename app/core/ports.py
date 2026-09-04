from abc import ABC, abstractmethod

from app.core.models import ChangeBatch, Mutation, MutationResult

""" Abstraction boundary for the remote store that will be used to apply mutations and retrieve changes. """

class RemoteStore(ABC):
    @abstractmethod
    async def apply_mutations(
        self,
        mutations: list[Mutation],
    ) -> list[MutationResult]:
        raise NotImplementedError

    @abstractmethod
    async def get_changes(
        self,
        cursor: int,
        limit: int = 100,
    ) -> ChangeBatch:
        raise NotImplementedError

    @abstractmethod
    async def health_check(self) -> bool:
        raise NotImplementedError