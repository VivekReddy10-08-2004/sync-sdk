from .client import SyncClient, SyncReport
from .gateway import SyncGateway
from .models import Change, ChangeBatch, Mutation, MutationResult
from .ports import (
    GatewayStore,
    GatewayTransaction,
    LocalStore,
    MutationRejected,
    RetryableError,
    SyncTransport,
)
from .registry import Entity, EntityRegistry
from .store import SQLiteStore

__all__ = [
    "Change",
    "ChangeBatch",
    "Entity",
    "EntityRegistry",
    "GatewayStore",
    "GatewayTransaction",
    "LocalStore",
    "Mutation",
    "MutationRejected",
    "MutationResult",
    "RetryableError",
    "SQLiteStore",
    "SyncClient",
    "SyncGateway",
    "SyncReport",
    "SyncTransport",
]


def main() -> None:
    print("Use SyncClient and SQLiteStore to connect to a sync gateway.")
