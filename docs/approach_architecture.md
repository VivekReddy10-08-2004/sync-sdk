# Setup and adapter guide

The app chooses what data to store and where to store it. The SDK moves that data
and keeps track of what was sent.

This guide explains the less common setup choices and the rules for connecting
a new database. Start with the [README](../README.md) for the basic setup.

## Names used in the code

| Name | Meaning |
| --- | --- |
| Entity | A name for a kind of data, such as `tasks` |
| Mutation | One request to create, update, or delete a row |
| Payload | The values sent with that request |
| Outbox | The local queue of changes waiting to be sent |
| Gateway | The server part that checks and saves changes |
| Adapter | Code that connects the pipeline to a database or transport |
| Binding | The rules that connect data fields to table columns |
| Cursor or checkpoint | The saved position in the server change list |
| Transaction | A group of database writes that all save or all undo together |

## What each part does

```text
Your app
  -> local queue
  -> sync client
  -> HTTP or a direct call
  -> gateway
  -> chosen database adapter
  -> your tables and the SDK tracking tables
```

The main sync code does not need FastAPI or SQLAlchemy. Those are optional parts.
The base package uses Pydantic to check data.

## Choose fields and data checks

`SQLAlchemyConfig.register_table()` connects a table to a sync name. It also sets
up the field checks. `config.build(sessions)` uses those settings to make a gateway.
Later registrations do not change a gateway you already built.

Use `fields={"sent_name": "column_name"}` to choose fields. Leaving out `fields`
includes every column except the row's key. A table can contain just key columns.
For example, a membership table may contain only a user ID and a group ID.

The generated checks use column types and whether a column allows `None`. Extra
fields are rejected. Simple Python default values can fill missing fields. A
selected field with no such default must be sent, even if it allows `None`.

Defaults calculated by a function or by the database are not added to sent data.
Your app must supply those values, leave those columns out of sync, or use custom
write code. If the database changes a synced value during a write, custom code
must also put the final value in the change list.

Pass `schema=YourPydanticModel` to use your own data checks. Use
`ConfigDict(extra="forbid")` in that model to reject extra fields.

## Keys made from several columns

A row can use one key column or several key columns. Use the returned binding to
make the ID you send:

```python
# For a table whose key is one integer column:
row_id = task_binding.encode_key(7)

# For a table whose key uses both of these columns:
row_id = account_binding.encode_key({"tenant": "acme", "account_number": 7})
```

For one key column, the ID is a string such as `"7"`. For several columns, the ID
is a JSON object with sorted names and string values. `encode_key()` makes that
format for you. It gives the same row the same ID each time.

The adapter rejects other forms of that ID. For example, integer key `"07"` is not
accepted as another name for `"7"`. The current SQL tracking tables allow up to
191 characters in the full ID.

Use `key_parsers` to supply a reader for each key column when its type needs special
handling. Use `value_decoders` to convert custom field values before writing them.
For custom column types, also supply a Pydantic schema for the sent values.

## Create tables and keep their links

Describe your tables with SQLAlchemy. You can include foreign keys, indexes,
column rules, and default values.

`register_table()` and `build()` do not read or change the database.
`await config.create_tables(engine)` creates only the registered tables and the
SDK tracking tables. SQLAlchemy handles the order needed to create linked tables,
including foreign keys that must be added after table creation.

Existing tables are left alone. Unregistered tables are not created. A table your
new table points to must be registered or already exist in the database.

Run setup before sync workers start. Do not run it from several workers at once.
Use your app's migration tool to add columns or change existing tables.

## Send linked changes in the right order

The gateway writes changes in the order you send them. It does not sort them.

For example, if an order row refers to a customer row:

1. Create the customer before creating the order.
2. Delete the order before deleting the customer.

If a database rule blocks a write, the whole group of writes is undone. Pending
client changes remain in the queue.

The SDK does not create missing related rows for you. It also does not log changes
made by a database trigger or by an automatic delete of linked rows. Add custom
write code or change capture if your app needs those changes sent to clients.

## Change the conflict rule

The default rule compares `base_version`, when supplied, with the server version.
A different version causes a conflict.

To use another rule, set `Entity.conflict_policy(mutation, current_version)`, or
pass `conflict_policy=` when registering a table. The function returns:

- `None` to accept the change.
- A message string to return a conflict.

A corrected change needs a new mutation ID. Do not reuse an old ID with new data.

## Connect a different database

The interfaces are in [ports.py](../src/sync_sdk/ports.py). These are the methods
an adapter must provide:

| Interface | Job |
| --- | --- |
| `LocalStore` | Save queued changes, results, retry times, received rows, and progress |
| `SyncTransport` | Send changes with `push()` and fetch changes with `pull()` |
| `RemoteStore` | Provide the gateway's apply, fetch, and health methods |
| `GatewayStore` | Open a write transaction, read changes, and check the store |
| `GatewayTransaction` | Read saved results and versions; write data, versions, changes, and results |

For a failed send that can be tried again, a transport raises
`RetryableError(retry_after=...)`. Other errors and cancellation are passed back
to the caller.

The gateway accepts only names in `EntityRegistry`. Each name must have a matching
storage binding. A missing binding is a setup error; the gateway does not choose
a fallback table.

Other databases use these same interfaces. Their adapters own the connection and
table or collection setup. The main pipeline does not need database-specific code.

## Rules every storage adapter must follow

1. Save each group's data, row versions, change entries, and results together.
   A failed group must not leave only some of those writes saved.
2. If the same mutation ID arrives again, return its saved result. Do not apply it
   again. This must also hold when two requests arrive at the same time.
3. Keep each mutation ID unique within the store. It must always refer to the same
   request and client.
4. Check a row's version and save its new value as one protected step. Two writers
   must not both accept the same old version. Keep the version after a row is deleted.
5. Give saved changes increasing numbers in the order they become visible. Once a
   client sees number 10, number 9 must not appear later. A database-generated ID
   alone is not enough to promise this order.
6. Fetch only changes after the requested cursor. Return them in order. Return the
   last number sent, or the input cursor if the list is empty. Set `has_more` when
   more changes remain.
7. On the client, save received rows and their cursor together. A restart must not
   keep a new cursor while losing the rows that belong to it.

If a connection is lost during a save, the caller may not know whether it finished.
Retrying the same IDs must be safe whether the save completed or not.

These rules apply to SQL and NoSQL. If a database cannot save the needed writes
together, its adapter needs another tested way to recover from a partial write.

## How the supplied SQL adapter works

`SQLAlchemyStore` uses the async sessions your app supplies. `TableBinding` reads
the row ID, converts field values, and writes the chosen table columns.

For writes that affect several tables, supply your own
`SQLAlchemyBinding.write(session, mutation, version)`. Use the provided session.
Do not commit inside the binding or write to an outside service.

A binding may raise `MutationRejected` only before it changes any data for that
mutation. Other write errors must stop and undo the whole group.

The SDK keeps its tracking data in separate tables:

| Table | What it keeps |
| --- | --- |
| `sync_clock` | The last server change number |
| `sync_versions` | The last version of each row, including deleted rows |
| `sync_processed_mutations` | Results for requests already handled |
| `sync_change_log` | The list of server changes |

`initialize_sync_metadata(engine)` creates only these tables and the clock's
starting row. If your own migrations create them, include `sync_metadata` and
start `sync_clock` with ID `1` and sequence `0` for a new store.

The SQL adapter lets one gateway group write at a time. PostgreSQL and MySQL use
a lock on the clock row; SQLite uses `BEGIN IMMEDIATE`. This keeps change numbers
in the right order. Changing this to allow more writers needs another way to keep
that promise.

Your data and the tracking tables must share one database transaction. MySQL tables
must use a storage engine that supports transactions. Writes made outside the
pipeline do not use its lock or enter its change list.

Rows that already exist are not sent automatically. A row with no sync history
starts at sync version zero.

## Add the gateway to an app

`HTTPTransport` sends requests over HTTP and reads `Retry-After`.
`DirectTransport` calls the gateway in the same process.
`create_sync_router()` adds sync routes to an existing FastAPI app.

Your app checks who is allowed to call the routes and which data they may see.
A gateway returns its whole store's change list. Use separate stores or a custom
adapter to keep different users' data apart.

`SyncClient(local, http)` still works. You can also pass `transport=` and use any
store that follows `LocalStore`.

## The sample app and its database update

The `app/` folder registers `task` and uses its sample `Record` table. The old
`PostgresRemoteStore` name is kept so earlier imports still work. It now calls the
shared gateway code.

The database update copies old change entries and results into the SDK tracking
tables. It keeps their change numbers so saved client positions still work. It
uses the latest old change to set each row's version, including deleted rows.
If a row has no logged change, it uses the version saved on that row.

## What has been tested

Tests run the HTTP gateway and client against SQLite. A simple store that holds
documents in memory runs the same adapter tests. That store is only for tests;
it is not a ready-to-use NoSQL adapter.

Tests cover repeated sends, competing writes, conflicts, failed groups of writes,
saved rejections, table creation, linked tables, direct calls, HTTP, and restarts.
They also check that old database data still works after the update.

Table creation SQL is checked for SQLite, PostgreSQL, and MySQL. Live PostgreSQL
and MySQL tests, ready-to-use NoSQL adapters, automatic tracking of outside writes,
and automatic sending of existing rows still need separate work.
