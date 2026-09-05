"""Run: uv run --extra sqlite python examples/pluggable_tasks.py

The demo uses temporary files and removes them when it finishes.
In your app, use your own database connection and create tables before starting sync.
"""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import BaseModel, ConfigDict
from sqlalchemy import Column, Integer, MetaData, String, Table, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from sync_sdk import SQLiteStore, SyncClient
from sync_sdk.adapters.direct import DirectTransport
from sync_sdk.adapters.sqlalchemy_config import SQLAlchemyConfig

# The application owns this table, its names, and its fields.
business_metadata = MetaData()
tasks = Table(
    "my_tasks",
    business_metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String(200), nullable=False),
    Column("internal_note", String(200), server_default="private"),
)


class TaskPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str


def configure() -> SQLAlchemyConfig:
    config = SQLAlchemyConfig()
    config.register_table("tasks", tasks, fields={"title": "title"}, schema=TaskPayload)
    return config


async def main() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        engine = create_async_engine(f"sqlite+aiosqlite:///{root / 'server.db'}")
        try:
            config = configure()
            # Create the tables we chose and the SDK tracking tables.
            await config.create_tables(engine)
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            gateway = config.build(sessions)
            with SQLiteStore(root / "local.db", client_id="device-1") as local:
                sync = SyncClient(local, transport=DirectTransport(gateway))
                sync.enqueue(
                    entity="tasks",
                    entity_id="1",
                    operation="create",
                    payload={"title": "My application's task"},
                    base_version=0,
                )
                await sync.sync_once()
                print(local.get_record("tasks", "1"))
            async with sessions() as session:
                print(dict((await session.execute(select(tasks))).mappings().one()))
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
