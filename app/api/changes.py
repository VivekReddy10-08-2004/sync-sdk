from fastapi import APIRouter, Depends, Query

from app.adapters.postgres import PostgresRemoteStore
from app.api.dependencies import get_remote_store
from app.core.models import ChangeBatch


router = APIRouter(prefix="/v1/sync", tags=["sync"])


@router.get("/changes", response_model=ChangeBatch)
async def pull_changes(
    cursor: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    store: PostgresRemoteStore = Depends(get_remote_store),
) -> ChangeBatch:
    return await store.get_changes(cursor, limit)