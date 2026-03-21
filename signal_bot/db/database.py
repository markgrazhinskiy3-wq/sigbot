import aiosqlite
import logging
from datetime import datetime

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger(__name__)

DB_PATH = config.DB_PATH


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                username    TEXT,
                status      TEXT NOT NULL DEFAULT 'pending',
                created_at  TEXT NOT NULL
            )
            """
        )
        await db.commit()
    logger.info("Database initialized at %s", DB_PATH)


async def add_or_get_user(user_id: int, username: str | None) -> tuple[dict, bool]:
    """
    Returns (user_dict, is_new).
    is_new=True only when the user was just inserted for the first time.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row), False

        now = datetime.utcnow().isoformat()
        await db.execute(
            "INSERT INTO users (user_id, username, status, created_at) VALUES (?, ?, 'pending', ?)",
            (user_id, username or "", now),
        )
        await db.commit()
        logger.info("New user registered: %s (%s)", username, user_id)
        return {
            "user_id": user_id,
            "username": username or "",
            "status": "pending",
            "created_at": now,
        }, True


async def get_status(user_id: int) -> str | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT status FROM users WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def set_status(user_id: int, status: str) -> bool:
    if status not in ("pending", "approved", "denied"):
        raise ValueError(f"Invalid status: {status}")
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "UPDATE users SET status = ? WHERE user_id = ?", (status, user_id)
        )
        await db.commit()
        return cursor.rowcount > 0


async def list_all() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users ORDER BY created_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def list_pending() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE status = 'pending' ORDER BY created_at ASC"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def list_approved() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE status = 'approved'"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]
