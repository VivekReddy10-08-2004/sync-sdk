from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import BaseModel, ConfigDict

from sync_sdk import Mutation, MutationRejected
from sync_sdk.adapters.mongodb import MongoCollectionBinding, MongoDBConfig


class TaskPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str


class FakeCollection:
    def __init__(self):
        self.update = None
        self.deleted = None

    async def update_one(self, query, update, **options):
        self.update = (query, update, options)

    async def delete_one(self, query, **options):
        self.deleted = (query, options)


class FakeDatabase:
    def __init__(self):
        self.collections = {"tasks": FakeCollection()}

    def __getitem__(self, name):
        return self.collections[name]


def mutation(entity_id="1", *, operation="create", payload=None):
    return Mutation(
        mutation_id=uuid4(),
        client_id="client",
        entity="tasks",
        entity_id=entity_id,
        operation=operation,
        payload={"title": "hello"} if payload is None else payload,
        base_version=0,
        created_at=datetime.now(UTC),
    )


async def test_collection_binding_maps_only_registered_fields():
    database = FakeDatabase()
    session = object()
    binding = MongoCollectionBinding(
        "tasks", fields={"title": "display_title"}, parse_key=int
    )
    await binding.write(database, session, mutation(), 1)
    query, update, options = database.collections["tasks"].update
    assert query == {"_id": 1}
    assert update == {"$set": {"display_title": "hello"}}
    assert options == {"upsert": True, "session": session}

    await binding.write(database, object(), mutation(operation="delete", payload={}), 2)
    assert database.collections["tasks"].deleted[0] == {"_id": 1}


async def test_collection_binding_rejects_bad_keys_and_payloads():
    database = FakeDatabase()
    binding = MongoCollectionBinding("tasks", fields={"title": "title"}, parse_key=int)
    with pytest.raises(MutationRejected, match="entity ID"):
        await binding.write(database, object(), mutation("01"), 1)
    with pytest.raises(MutationRejected, match="exactly"):
        await binding.write(database, object(), mutation(payload={"extra": True}), 1)


def test_mongodb_config_rejects_ambiguous_or_reserved_collections():
    config = MongoDBConfig()
    binding = config.register_collection("tasks", "app_tasks", schema=TaskPayload)
    assert binding.fields == {"title": "title"}
    with pytest.raises(ValueError, match="once"):
        config.register_collection(
            "task_alias", "app_tasks", fields={"title": "title"}
        )
    with pytest.raises(ValueError, match="metadata"):
        MongoDBConfig().register_collection(
            "clock", "sync_clock", fields={"value": "value"}
        )
    with pytest.raises(ValueError, match="fields or a schema"):
        MongoDBConfig().register_collection("empty", "empty")


@pytest.mark.parametrize("document_field", ["_id", "$secret", "nested.value"])
def test_mongodb_binding_rejects_unsafe_document_fields(document_field):
    with pytest.raises(ValueError, match="top-level"):
        MongoCollectionBinding("tasks", fields={"value": document_field})
