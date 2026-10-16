# MongoDB adapter

The MongoDB adapter uses PyMongo's native async driver and the same gateway,
validation, conflict, HTTP, and client pipeline as the SQL adapter.

## Requirements

- MongoDB must run as a replica set or sharded cluster. Standalone servers do not
  support the multi-document transactions required by the storage contract.
- Business collections and SDK metadata collections must be in the same database.
- Initialize collections before starting gateway workers.
- Reuse one `AsyncMongoClient` per event loop and close it during application
  shutdown.

Install the optional driver with the server dependencies you use:

```sh
pip install 'sync-sdk[mongodb,http,fastapi]'
```

## Configure collections

Register each physical collection once and explicitly map the fields clients may
sync. Unmapped document fields remain private and are left unchanged.

```python
from pydantic import BaseModel, ConfigDict
from pymongo import AsyncMongoClient

from sync_sdk.adapters.mongodb import MongoDBConfig


class TaskPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str
    completed: bool


client = AsyncMongoClient("mongodb://localhost:27017/?replicaSet=rs0")
database = client["application"]

config = MongoDBConfig()
task_binding = config.register_collection(
    "tasks",
    "app_tasks",
    fields={"title": "display_title", "completed": "is_complete"},
    schema=TaskPayload,
)

await config.create_collections(database)
gateway = config.build(database)
```

`register_collection()` and `build()` perform no database I/O.
`create_collections()` creates missing registered collections and the SDK metadata
collections, then initializes the clock. It leaves existing collections, indexes,
validators, and documents unchanged.

The default document ID is the wire `entity_id` string. Use `parse_key` for another
MongoDB `_id` type and use the returned binding to produce its canonical wire ID:

```python
from bson import ObjectId

binding = config.register_collection(
    "notes",
    "notes",
    fields={"body": "body"},
    parse_key=ObjectId,
)
entity_id = binding.encode_key(ObjectId())
```

For custom value types, pass `value_decoders={"field": decoder}`. Decoders run
before the mapped values are written in the gateway transaction.

## Existing application collections

If the application creates business collections itself, initialize only SDK
metadata:

```python
from sync_sdk.adapters.mongodb import initialize_mongodb_metadata

await initialize_mongodb_metadata(database)
```

The default metadata collections are `sync_clock`, `sync_versions`,
`sync_processed_mutations`, and `sync_change_log`. Set `metadata_prefix` on both
configuration and metadata initialization when a different prefix is required.

## Guarantees and operating behavior

Each gateway batch writes business documents, versions, results, and feed entries
in one MongoDB transaction. Updating the clock document serializes writers and
keeps feed sequences in commit order. The adapter retains versions after deletes
and uses mutation IDs as unique processed-result keys.

One adapter instance also serializes its local writers. If separate application
instances contend on the clock document, MongoDB can abort one transaction; the
adapter reports labeled transaction conflicts as retryable. Retrying the original
mutation IDs is safe.

Mapped fields are updated with `$set`, so private fields remain intact. Deletes
remove the complete business document. Direct writes, database-trigger equivalents,
and MongoDB change streams are outside this adapter and do not enter the sync feed.

Use separate databases or an authorization-aware custom adapter when tenants must
not share a change feed. The standard gateway returns all changes in its database.
