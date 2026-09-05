from fastapi import FastAPI

from app.api.dependencies import get_remote_store
from sync_sdk.integrations.fastapi import create_sync_router

app = FastAPI(
    title="Offline Sync Gateway",
    version="0.1.0",
    description="Offline-first synchronization gateway for local clients and central databases.",
)


app.include_router(create_sync_router(get_remote_store))


@app.get("/")
async def root():
    return {
        "service": "offline-sync-gateway",
        "version": "0.1.0",
        "status": "running",
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
    }
