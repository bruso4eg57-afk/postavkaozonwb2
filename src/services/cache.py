from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class CacheRepository:
    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._init()

    def _init(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                ts TEXT NOT NULL,
                payload TEXT NOT NULL,
                is_success INTEGER NOT NULL,
                hash TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS validations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                level TEXT NOT NULL,
                check_name TEXT NOT NULL,
                details TEXT
            )
            """
        )
        self.conn.commit()

    def save_snapshot(self, source: str, ts: str, payload: Any, is_success: bool, payload_hash: str = "") -> None:
        self.conn.execute(
            "INSERT INTO snapshots(source, ts, payload, is_success, hash) VALUES (?, ?, ?, ?, ?)",
            (source, ts, json.dumps(payload, ensure_ascii=False), int(is_success), payload_hash),
        )
        self.conn.commit()

    def latest_success(self, source: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT ts, payload, hash FROM snapshots WHERE source=? AND is_success=1 ORDER BY id DESC LIMIT 1",
            (source,),
        ).fetchone()
        if not row:
            return None
        return {"ts": row[0], "payload": json.loads(row[1]), "hash": row[2]}

    def save_validation(self, ts: str, level: str, check_name: str, details: str) -> None:
        self.conn.execute(
            "INSERT INTO validations(ts, level, check_name, details) VALUES (?, ?, ?, ?)",
            (ts, level, check_name, details),
        )
        self.conn.commit()
