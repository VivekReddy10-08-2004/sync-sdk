from typing import Annotated

from fastapi import APIRouter, Depends

from app.api.dependencies import get_remote_store
from app.core.models import Mutation, MutationResult
from sync_sdk.ports import RemoteStore

router = APIRouter(prefix="/v1/sync", tags=["sync"])


@router.post("/mutations", response_model=list[MutationResult])
async def push_mutations(
    mutations: list[Mutation],
    store: Annotated[RemoteStore, Depends(get_remote_store)],
) -> list[MutationResult]:
    return await store.apply_mutations(mutations)
