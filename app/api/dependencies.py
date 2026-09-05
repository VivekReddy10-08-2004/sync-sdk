from app.adapters.postgres import PostgresRemoteStore
from sync_sdk.ports import RemoteStore


def get_remote_store() -> RemoteStore:
    return PostgresRemoteStore()
