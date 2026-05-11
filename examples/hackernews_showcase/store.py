"""Thread-safe SQLite persistence for the showcase."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

from .models import ItemRecord, UserRecord


class SQLiteStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._lock = threading.Lock()

    def create_schema(self) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY,
                    type TEXT NOT NULL,
                    by_user TEXT,
                    title TEXT,
                    text TEXT,
                    score INTEGER NOT NULL,
                    url TEXT,
                    parent INTEGER,
                    poll INTEGER,
                    kids TEXT,
                    parts TEXT,
                    descendants INTEGER NOT NULL,
                    time INTEGER NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    created INTEGER NOT NULL,
                    karma INTEGER NOT NULL,
                    about TEXT,
                    submitted TEXT
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
                    name TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

    def save_item(self, data: dict[str, Any]) -> int:
        record = ItemRecord.from_hn(data)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO items (
                    id, type, by_user, title, text, score, url, parent, poll,
                    kids, parts, descendants, time
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                record.as_row(),
            )
        return record.id

    def save_user(self, data: dict[str, Any]) -> str:
        record = UserRecord.from_hn(data)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO users (id, created, karma, about, submitted)
                VALUES (?, ?, ?, ?, ?)
                """,
                record.as_row(),
            )
        return record.id

    def save_metric(self, name: str, value: object) -> str:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR REPLACE INTO metrics (name, value) VALUES (?, ?)",
                (name, str(value)),
            )
        return name

    def counts(self) -> dict[str, int]:
        with self._lock:
            return {
                "items": self._connection.execute("SELECT count(*) FROM items").fetchone()[0],
                "users": self._connection.execute("SELECT count(*) FROM users").fetchone()[0],
                "metrics": self._connection.execute("SELECT count(*) FROM metrics").fetchone()[0],
            }

    def close(self) -> None:
        with self._lock:
            self._connection.close()
