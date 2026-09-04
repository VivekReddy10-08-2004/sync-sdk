"""Durable local state. Transactions contain no network calls or await points."""

import json
import sqlite3
from pathlib import Path

from .models import ChangeBatch, Mutation, MutationResult


class SQLiteStore:
    """Use one database per client and remote dataset; one sync worker per file."""

    def __init__(self, path: str | Path, *, client_id: str):
        self.db = sqlite3.connect(str(path))
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (
                position INTEGER PRIMARY KEY AUTOINCREMENT,
                mutation_id TEXT UNIQUE NOT NULL, body TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', result TEXT
            );
            CREATE TABLE IF NOT EXISTS records (
                entity TEXT NOT NULL, entity_id TEXT NOT NULL,
                payload TEXT NOT NULL, version INTEGER NOT NULL,
                PRIMARY KEY(entity, entity_id)
            );
        """)
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO metadata VALUES ('client_id', ?)", (client_id,)
            )
        if self._get("client_id") != client_id:
            self.db.close()
            raise ValueError("Database belongs to a different client_id")
        self.client_id = client_id

    def _get(self, key: str, default: str = "0") -> str:
        row = self.db.execute(
            "SELECT value FROM metadata WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else default

    def _set(self, key: str, value: str) -> None:
        self.db.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)", (key, value))

    @property
    def cursor(self) -> int:
        return int(self._get("cursor"))

    @property
    def retry_state(self) -> tuple[int, float]:
        return int(self._get("attempts")), float(self._get("retry_at"))

    def schedule_retry(self, attempts: int, retry_at: float) -> None:
        with self.db:
            self._set("attempts", str(attempts))
            self._set("retry_at", str(retry_at))

    def enqueue(self, mutation: Mutation) -> None:
        if mutation.client_id != self.client_id:
            raise ValueError("Mutation client_id does not match database")
        body = mutation.model_dump_json()
        with self.db:
            row = self.db.execute(
                "SELECT body FROM outbox WHERE mutation_id=?",
                (str(mutation.mutation_id),),
            ).fetchone()
            if row and row[0] != body:
                raise ValueError("Cannot reuse a mutation_id with different content")
            self.db.execute(
                "INSERT OR IGNORE INTO outbox (mutation_id, body) VALUES (?, ?)",
                (str(mutation.mutation_id), body),
            )

    def pending(self, limit: int) -> list[Mutation]:
        rows = self.db.execute(
            "SELECT body FROM outbox WHERE status='pending' ORDER BY position LIMIT ?",
            (limit,),
        )
        return [Mutation.model_validate_json(row[0]) for row in rows]

    def results(self) -> list[MutationResult]:
        rows = self.db.execute(
            "SELECT result FROM outbox WHERE result IS NOT NULL ORDER BY position"
        )
        return [MutationResult.model_validate_json(row[0]) for row in rows]

    def acknowledge(self, results: list[MutationResult]) -> None:
        with self.db:
            for result in results:
                self.db.execute(
                    "UPDATE outbox SET status=?, result=? WHERE mutation_id=? AND status='pending'",
                    (result.status, result.model_dump_json(), str(result.mutation_id)),
                )

    def apply_page(self, page: ChangeBatch) -> None:
        """Validate progress, then commit the entire page and checkpoint atomically."""
        cursor = self.cursor
        for change in page.changes:
            if change.sequence <= cursor:
                raise ValueError("Change sequences must strictly increase")
            cursor = change.sequence
        if page.cursor != cursor or (page.has_more and not page.changes):
            raise ValueError("Invalid change feed checkpoint")
        with self.db:
            for change in page.changes:
                if change.operation == "delete":
                    self.db.execute(
                        "DELETE FROM records WHERE entity=? AND entity_id=?",
                        (change.entity, change.entity_id),
                    )
                else:
                    self.db.execute(
                        "INSERT OR REPLACE INTO records VALUES (?, ?, ?, ?)",
                        (
                            change.entity,
                            change.entity_id,
                            json.dumps(change.payload),
                            change.version,
                        ),
                    )
            self._set("cursor", str(page.cursor))

    def get_record(self, entity: str, entity_id: str) -> dict | None:
        row = self.db.execute(
            "SELECT payload, version FROM records WHERE entity=? AND entity_id=?",
            (entity, entity_id),
        ).fetchone()
        return {"payload": json.loads(row[0]), "version": row[1]} if row else None

    def close(self) -> None:
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
