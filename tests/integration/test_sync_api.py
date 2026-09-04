from datetime import UTC, datetime
from uuid import uuid4

import pytest


pytestmark = pytest.mark.asyncio


def make_mutation(
    *,
    mutation_id=None,
    entity="task",
    entity_id="task-1",
    operation="create",
    payload=None,
    base_version=None,
):
    return {
        "mutation_id": str(mutation_id or uuid4()),
        "client_id": "test-client",
        "entity": entity,
        "entity_id": entity_id,
        "operation": operation,
        "payload": payload or {"status": "open"},
        "base_version": base_version,
        "created_at": datetime.now(UTC).isoformat(),
    }


async def test_create_mutation_is_applied(client):
    mutation = make_mutation()

    response = await client.post(
        "/v1/sync/mutations",
        json=[mutation],
    )

    assert response.status_code == 200

    body = response.json()

    assert len(body) == 1
    assert body[0]["status"] == "applied"
    assert body[0]["server_version"] == 1
    assert body[0]["mutation_id"] == mutation["mutation_id"]


async def test_duplicate_mutation_is_idempotent(client):
    mutation_id = uuid4()

    mutation = make_mutation(
        mutation_id=mutation_id,
    )

    first = await client.post(
        "/v1/sync/mutations",
        json=[mutation],
    )

    second = await client.post(
        "/v1/sync/mutations",
        json=[mutation],
    )

    assert first.status_code == 200
    assert second.status_code == 200

    first_body = first.json()[0]
    second_body = second.json()[0]

    assert first_body == second_body
    assert first_body["status"] == "applied"
    assert first_body["server_version"] == 1


async def test_update_with_correct_base_version(client):
    create_mutation = make_mutation(
        entity_id="task-2",
        payload={"status": "open"},
    )

    create_response = await client.post(
        "/v1/sync/mutations",
        json=[create_mutation],
    )

    assert create_response.status_code == 200
    assert create_response.json()[0]["server_version"] == 1

    update_mutation = make_mutation(
        entity_id="task-2",
        operation="update",
        payload={"status": "done"},
        base_version=1,
    )

    update_response = await client.post(
        "/v1/sync/mutations",
        json=[update_mutation],
    )

    assert update_response.status_code == 200

    body = update_response.json()[0]

    assert body["status"] == "applied"
    assert body["server_version"] == 2


async def test_wrong_base_version_returns_conflict(client):
    create_mutation = make_mutation(
        entity_id="task-3",
    )

    await client.post(
        "/v1/sync/mutations",
        json=[create_mutation],
    )

    conflicting_mutation = make_mutation(
        entity_id="task-3",
        operation="update",
        payload={"status": "done"},
        base_version=999,
    )

    response = await client.post(
        "/v1/sync/mutations",
        json=[conflicting_mutation],
    )

    assert response.status_code == 200

    body = response.json()[0]

    assert body["status"] == "conflict"
    assert body["server_version"] == 1
    assert body["message"] == "Version mismatch"


async def test_change_feed_returns_new_changes(client):
    mutation = make_mutation(
        entity_id="task-4",
    )

    await client.post(
        "/v1/sync/mutations",
        json=[mutation],
    )

    response = await client.get(
        "/v1/sync/changes",
        params={"cursor": 0},
    )

    assert response.status_code == 200

    body = response.json()

    assert len(body["changes"]) == 1
    assert body["changes"][0]["entity"] == "task"
    assert body["changes"][0]["entity_id"] == "task-4"
    assert body["changes"][0]["version"] == 1
    assert body["cursor"] >= 1


async def test_change_feed_cursor_only_returns_newer_changes(client):
    first = make_mutation(
        entity_id="task-5",
    )

    await client.post(
        "/v1/sync/mutations",
        json=[first],
    )

    first_pull = await client.get(
        "/v1/sync/changes",
        params={"cursor": 0},
    )

    assert first_pull.status_code == 200

    first_body = first_pull.json()
    first_cursor = first_body["cursor"]

    second = make_mutation(
        entity_id="task-6",
    )

    await client.post(
        "/v1/sync/mutations",
        json=[second],
    )

    second_pull = await client.get(
        "/v1/sync/changes",
        params={"cursor": first_cursor},
    )

    assert second_pull.status_code == 200

    second_body = second_pull.json()

    assert len(second_body["changes"]) == 1
    assert second_body["changes"][0]["entity_id"] == "task-6"
    assert second_body["cursor"] > first_cursor


async def test_delete_generates_change_event(client):
    create_mutation = make_mutation(
        entity_id="task-delete",
    )

    await client.post(
        "/v1/sync/mutations",
        json=[create_mutation],
    )

    delete_mutation = make_mutation(
        entity_id="task-delete",
        operation="delete",
        payload={},
        base_version=1,
    )

    response = await client.post(
        "/v1/sync/mutations",
        json=[delete_mutation],
    )

    assert response.status_code == 200

    result = response.json()[0]

    assert result["status"] == "applied"
    assert result["server_version"] == 2

    changes_response = await client.get(
        "/v1/sync/changes",
        params={"cursor": 0},
    )

    changes = changes_response.json()["changes"]

    assert len(changes) == 2
    assert changes[-1]["operation"] == "delete"
    assert changes[-1]["entity_id"] == "task-delete"