"""Embed the service without HTTP or a web framework."""

from ..models import ChangeBatch, Mutation, MutationResult
from ..ports import RemoteStore


class DirectTransport:
    def __init__(self, gateway: RemoteStore):
        self.gateway = gateway

    async def push(self, mutations: list[Mutation]) -> list[MutationResult]:
        return await self.gateway.apply_mutations(mutations)

    async def pull(self, cursor: int, limit: int) -> ChangeBatch:
        return await self.gateway.get_changes(cursor, limit)
