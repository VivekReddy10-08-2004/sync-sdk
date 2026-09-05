"""Mount sync routes in a developer's existing FastAPI application."""

from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from ..models import ChangeBatch, Mutation, MutationResult
from ..ports import RemoteStore


def create_sync_router(
    get_gateway: Callable[..., RemoteStore], *, prefix: str = "/v1/sync"
) -> APIRouter:
    """Dependency providers may apply application authentication/tenant scoping."""
    router = APIRouter(prefix=prefix, tags=["sync"])

    @router.post("/mutations", response_model=list[MutationResult])
    async def push(
        mutations: list[Mutation], gateway: Annotated[RemoteStore, Depends(get_gateway)]
    ):
        return await gateway.apply_mutations(mutations)

    @router.get("/changes", response_model=ChangeBatch)
    async def pull(
        gateway: Annotated[RemoteStore, Depends(get_gateway)],
        cursor: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=1000),
    ):
        return await gateway.get_changes(cursor, limit)

    return router
