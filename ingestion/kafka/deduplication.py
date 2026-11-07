"""Idempotencja: zapamiętuje przetworzone event_id, żeby ponowna dostawa nie tworzyła duplikatów.
Kafka daje gwarancję 'at-least-once', więc duplikaty są normalne i trzeba je obsłużyć."""
import sqlite3
from datetime import datetime, timezone


class DedupStore:
    def __init__(self, path: str = ":memory:"):
        self._conn = sqlite3.connect(path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id     TEXT PRIMARY KEY,
                processed_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def is_processed(self, event_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return row is not None

    def mark_processed(self, event_ids: list[str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.executemany(
            "INSERT OR IGNORE INTO processed_events (event_id, processed_at) VALUES (?, ?)",
            [(event_id, now) for event_id in event_ids],
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()