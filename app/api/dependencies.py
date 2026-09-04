from app.adapters.postgres import PostgresRemoteStore


def get_remote_store() -> PostgresRemoteStore:
    return PostgresRemoteStore()