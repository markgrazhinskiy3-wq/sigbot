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
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_outcomes (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id        INTEGER NOT NULL,
                symbol         TEXT NOT NULL,
                pair_label     TEXT NOT NULL,
                direction      TEXT NOT NULL,
                confidence     INTEGER,
                strategy       TEXT,
                expiration_sec INTEGER NOT NULL,
                signal_price   REAL NOT NULL,
                result_price   REAL,
                outcome        TEXT NOT NULL DEFAULT 'pending',
                created_at     TEXT NOT NULL,
                resolved_at    TEXT
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


# ── Signal outcome tracking ───────────────────────────────────────────────────

async def save_signal_outcome(
    user_id: int,
    symbol: str,
    pair_label: str,
    direction: str,
    confidence: int,
    strategy: str | None,
    expiration_sec: int,
    signal_price: float,
) -> int:
    """Save a new pending signal. Returns the row id."""
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO signal_outcomes
                (user_id, symbol, pair_label, direction, confidence,
                 strategy, expiration_sec, signal_price, outcome, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (user_id, symbol, pair_label, direction, confidence,
             strategy, expiration_sec, signal_price, now),
        )
        await db.commit()
        return cursor.lastrowid


async def resolve_outcome(outcome_id: int, result_price: float, outcome: str) -> None:
    """Update a pending signal with actual result. outcome: 'win' | 'loss' | 'error'"""
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            UPDATE signal_outcomes
            SET result_price = ?, outcome = ?, resolved_at = ?
            WHERE id = ?
            """,
            (result_price, outcome, now, outcome_id),
        )
        await db.commit()


async def get_user_stats(user_id: int) -> dict:
    """Return win/loss stats for a user."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN outcome = 'win'  THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN outcome = 'pending' THEN 1 ELSE 0 END) as pending
            FROM signal_outcomes
            WHERE user_id = ?
            """,
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            total, wins, losses, pending = row or (0, 0, 0, 0)
            total   = total   or 0
            wins    = wins    or 0
            losses  = losses  or 0
            pending = pending or 0
            winrate = round(wins / (wins + losses) * 100) if (wins + losses) > 0 else None
            return {
                "total":   total,
                "wins":    wins,
                "losses":  losses,
                "pending": pending,
                "winrate": winrate,
            }


async def get_daily_admin_stats(date_prefix: str) -> dict:
    """
    Admin-only: aggregate stats for all signals issued on a given UTC date.
    date_prefix — ISO date string prefix, e.g. '2026-03-22'
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Overall totals
        async with db.execute(
            """
            SELECT
                COUNT(*)                                                AS total,
                COUNT(DISTINCT user_id)                                 AS unique_users,
                SUM(CASE WHEN outcome = 'win'     THEN 1 ELSE 0 END)   AS wins,
                SUM(CASE WHEN outcome = 'loss'    THEN 1 ELSE 0 END)   AS losses,
                SUM(CASE WHEN outcome = 'pending' THEN 1 ELSE 0 END)   AS pending,
                SUM(CASE WHEN direction = 'BUY'  THEN 1 ELSE 0 END)   AS buy_count,
                SUM(CASE WHEN direction = 'SELL' THEN 1 ELSE 0 END)    AS sell_count
            FROM signal_outcomes
            WHERE created_at LIKE ?
            """,
            (date_prefix + "%",),
        ) as cursor:
            row = await cursor.fetchone()
            totals = dict(row) if row else {}

        # By pair (top 8)
        async with db.execute(
            """
            SELECT
                pair_label,
                COUNT(*)                                              AS total,
                SUM(CASE WHEN outcome = 'win'  THEN 1 ELSE 0 END)   AS wins,
                SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END)   AS losses
            FROM signal_outcomes
            WHERE created_at LIKE ?
            GROUP BY pair_label
            ORDER BY total DESC
            LIMIT 8
            """,
            (date_prefix + "%",),
        ) as cursor:
            by_pair = [dict(r) for r in await cursor.fetchall()]

        # By strategy
        async with db.execute(
            """
            SELECT
                strategy,
                COUNT(*)                                              AS total,
                SUM(CASE WHEN outcome = 'win'  THEN 1 ELSE 0 END)   AS wins,
                SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END)   AS losses
            FROM signal_outcomes
            WHERE created_at LIKE ?
            GROUP BY strategy
            ORDER BY total DESC
            """,
            (date_prefix + "%",),
        ) as cursor:
            by_strategy = [dict(r) for r in await cursor.fetchall()]

    wins    = totals.get("wins")    or 0
    losses  = totals.get("losses")  or 0
    winrate = round(wins / (wins + losses) * 100) if (wins + losses) > 0 else None
    totals["winrate"] = winrate

    for row in by_pair + by_strategy:
        w = row.get("wins") or 0
        l = row.get("losses") or 0
        row["winrate"] = round(w / (w + l) * 100) if (w + l) > 0 else None

    return {
        "totals":      totals,
        "by_pair":     by_pair,
        "by_strategy": by_strategy,
    }


async def get_strategy_stats(user_id: int) -> list[dict]:
    """Return win/loss breakdown by strategy for a user."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT
                strategy,
                COUNT(*) as total,
                SUM(CASE WHEN outcome = 'win'  THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) as losses
            FROM signal_outcomes
            WHERE user_id = ? AND outcome IN ('win', 'loss')
            GROUP BY strategy
            ORDER BY total DESC
            """,
            (user_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            result = []
            for r in rows:
                w, l = (r["wins"] or 0), (r["losses"] or 0)
                result.append({
                    "strategy": r["strategy"] or "unknown",
                    "total":    r["total"],
                    "wins":     w,
                    "losses":   l,
                    "winrate":  round(w / (w + l) * 100) if (w + l) > 0 else None,
                })
            return result
