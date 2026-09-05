from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.api.dependencies import get_remote_store
from app.core.models import ChangeBatch
from sync_sdk.ports import RemoteStore

router = APIRouter(prefix="/v1/sync", tags=["sync"])


@router.get("/changes", response_model=ChangeBatch)
async def pull_changes(
    store: Annotated[RemoteStore, Depends(get_remote_store)],
    cursor: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
) -> ChangeBatch:
    return await store.get_changes(cursor, limit)
