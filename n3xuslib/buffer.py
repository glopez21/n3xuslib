import asyncio
import json
import logging
import sqlite3
import time
from pathlib import Path

logger = logging.getLogger("n3xuslib.buffer")


class SQLiteBuffer:
    def __init__(self, db_path: str):
        self._path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                sent INTEGER DEFAULT 0
            )"""
        )
        self._conn.commit()
        self._lock = asyncio.Lock()

    async def push(self, event: dict) -> None:
        async with self._lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: self._conn.execute(
                    "INSERT INTO outbox (event_json, created_at) VALUES (?, ?)",
                    (json.dumps(event), time.time()),
                ),
            )
            self._conn.commit()

    async def pop_pending(self, limit: int = 50) -> list[tuple[int, dict]]:
        async with self._lock:
            loop = asyncio.get_running_loop()
            rows = await loop.run_in_executor(
                None,
                lambda: self._conn.execute(
                    "SELECT id, event_json FROM outbox WHERE sent = 0 ORDER BY created_at LIMIT ?",
                    (limit,),
                ).fetchall(),
            )
            return [(r[0], json.loads(r[1])) for r in rows]

    async def mark_sent(self, ids: list[int]) -> None:
        if not ids:
            return
        async with self._lock:
            loop = asyncio.get_running_loop()
            placeholders = ",".join("?" for _ in ids)
            await loop.run_in_executor(
                None,
                lambda: self._conn.execute(
                    f"UPDATE outbox SET sent = 1 WHERE id IN ({placeholders})", ids
                ),
            )
            self._conn.commit()

    async def count_pending(self) -> int:
        loop = asyncio.get_running_loop()
        row = await loop.run_in_executor(
            None,
            lambda: self._conn.execute("SELECT count(*) FROM outbox WHERE sent = 0").fetchone(),
        )
        return row[0]

    def close(self) -> None:
        self._conn.close()
