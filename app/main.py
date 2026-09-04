from fastapi import FastAPI

from app.api.changes import router as changes_router
from app.api.mutations import router as mutations_router


app = FastAPI(
    title="Offline Sync Gateway",
    version="0.1.0",
    description="Offline-first synchronization gateway for local clients and central databases.",
)


app.include_router(mutations_router)
app.include_router(changes_router)


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