from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

""" Contracts for the mutation and change tracking system. """

class Mutation(BaseModel):
    mutation_id: UUID
    client_id: str
    entity: str
    entity_id: str
    operation: Literal["create", "update", "delete"]
    payload: dict[str, Any] = Field(default_factory=dict)
    base_version: int | None = None
    created_at: datetime


class MutationResult(BaseModel):
    mutation_id: UUID
    status: Literal["applied", "conflict", "rejected"]
    server_version: int | None = None
    message: str | None = None


class Change(BaseModel):
    sequence: int
    entity: str
    entity_id: str
    operation: Literal["create", "update", "delete"]
    payload: dict[str, Any]
    version: int
    created_at: datetime


class ChangeBatch(BaseModel):
    cursor: int
    has_more: bool
    changes: list[Change]