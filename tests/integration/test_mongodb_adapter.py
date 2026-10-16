"""Run the gateway storage contract against a live MongoDB replica set."""

import asyncio
import os
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

import pytest

pytest.importorskip("pymongo")

from pymongo import AsyncMongoClient

from sync_sdk import Mutation
from sync_sdk.adapters.mongodb import MongoDBConfig


def mutation(
    entity_id: str = "1",
    *,
    operation: Literal["create", "update", "delete"] = "create",
    base_version: int = 0,
    title: str = "offline task",
) -> Mutation:
    return Mutation(
        mutation_id=uuid4(),
        client_id="mongodb-adapter-test",
        entity="tasks",
        entity_id=entity_id,
        operation=operation,
        payload={} if operation == "delete" else {"title": title},
        base_version=base_version,
        created_at=datetime.now(UTC),
    )


@pytest.mark.skipif(
    not os.getenv("SYNC_TEST_MONGODB_URL"),
    reason="SYNC_TEST_MONGODB_URL is not configured",
)
async def test_mongodb_adapter_contract():
    client = AsyncMongoClient(os.environ["SYNC_TEST_MONGODB_URL"])
    database = client.get_default_database()
    if "test" not in database.name.lower():
        raise ValueError("Live adapter tests require a database name containing 'test'")

    config = MongoDBConfig(metadata_prefix="sync_test_")
    config.register_collection(
        "tasks", "sync_adapter_test_tasks", fields={"title": "display_title"}
    )
    try:
        await client.drop_database(database.name)
        await config.create_collections(database)
        await database["sync_adapter_test_tasks"].insert_one(
            {"_id": "1", "display_title": "existing", "private_note": "keep"}
        )
        gateway = config.build(database)

        create = mutation()
        repeated = await gateway.apply_mutations([create, create])
        assert repeated[0] == repeated[1]
        assert repeated[0].status == "applied"
        document = await database["sync_adapter_test_tasks"].find_one({"_id": "1"})
        assert document["private_note"] == "keep"

        first = mutation(operation="update", base_version=1, title="first")
        second = mutation(operation="update", base_version=1, title="second")
        left, right = await asyncio.gather(
            gateway.apply_mutations([first]), gateway.apply_mutations([second])
        )
        assert sorted([left[0].status, right[0].status]) == ["applied", "conflict"]

        first_page = await gateway.get_changes(0, limit=1)
        second_page = await gateway.get_changes(first_page.cursor, limit=10)
        assert first_page.has_more
        assert [change.version for change in second_page.changes] == [2]

        deleted = await gateway.apply_mutations(
            [mutation(operation="delete", base_version=2)]
        )
        assert deleted[0].server_version == 3
        assert (
            await database["sync_adapter_test_tasks"].find_one({"_id": "1"})
            is None
        )

        original = gateway.store.bindings["tasks"]

        class FailingBinding:
            collection_name = original.collection_name

            async def write(self, database, session, item, version):
                await original.write(database, session, item, version)
                if item.entity_id == "3":
                    raise RuntimeError("Simulated MongoDB business write failure")

        gateway.store.bindings["tasks"] = FailingBinding()
        with pytest.raises(RuntimeError, match="business write"):
            await gateway.apply_mutations([mutation("2"), mutation("3")])
        assert (
            await database["sync_adapter_test_tasks"].count_documents(
                {"_id": {"$in": ["2", "3"]}}
            )
            == 0
        )
        assert len((await gateway.get_changes(0)).changes) == 3
        assert await gateway.health_check()
    finally:
        await client.drop_database(database.name)
        await client.close()
