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
                user_id      INTEGER PRIMARY KEY,
                username     TEXT,
                status       TEXT NOT NULL DEFAULT 'pending',
                created_at   TEXT NOT NULL,
                auto_signals INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                user_id    INTEGER PRIMARY KEY,
                username   TEXT,
                added_by   INTEGER NOT NULL,
                added_at   TEXT NOT NULL
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
        # ── Migration: add performance-tracking columns if missing ──────────────
        # SQLite does not support ADD COLUMN IF NOT EXISTS, so we try each.
        _new_cols = [
            ("market_mode",     "TEXT"),
            ("used_tier",       "TEXT"),
            ("confidence_raw",  "REAL"),
            ("confidence_band", "TEXT"),
        ]
        for col, dtype in _new_cols:
            try:
                await db.execute(
                    f"ALTER TABLE signal_outcomes ADD COLUMN {col} {dtype}"
                )
            except Exception:
                pass  # column already exists

        # ── Condition frequency stats tables ────────────────────────────────
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_evals (
                strategy TEXT PRIMARY KEY,
                count    INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS condition_stats (
                strategy    TEXT    NOT NULL,
                condition   TEXT    NOT NULL,
                true_count  INTEGER NOT NULL DEFAULT 0,
                false_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (strategy, condition)
            )
            """
        )

        await db.commit()

        # ── Migrations: add columns to existing databases ──────────────────────
        try:
            await db.execute(
                "ALTER TABLE users ADD COLUMN auto_signals INTEGER NOT NULL DEFAULT 0"
            )
            await db.commit()
            logger.info("Migration applied: users.auto_signals column added")
        except Exception:
            pass  # column already exists

        try:
            await db.execute(
                "ALTER TABLE users ADD COLUMN lang TEXT NOT NULL DEFAULT 'ru'"
            )
            await db.commit()
            logger.info("Migration applied: users.lang column added")
        except Exception:
            pass  # column already exists

    logger.info("Database initialized at %s", DB_PATH)

    # Analytics tables (separate module — init after core tables)
    try:
        from services.analytics_logger import init_analytics
        await init_analytics()
    except Exception as _ae:
        logger.warning("Analytics init failed (non-critical): %s", _ae)


async def set_user_lang(user_id: int, lang: str) -> None:
    """Persist language preference for a user."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET lang = ? WHERE user_id = ?",
            (lang, user_id),
        )
        await db.commit()


async def load_all_user_langs() -> dict[int, str]:
    """Load lang preference for all users (for startup warm-up)."""
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            async with db.execute("SELECT user_id, lang FROM users") as cursor:
                rows = await cursor.fetchall()
                return {row[0]: row[1] for row in rows if row[1]}
        except Exception:
            return {}


async def get_auto_signals(user_id: int) -> bool:
    """Return True if the user has auto-signals enabled (persisted in DB)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT auto_signals FROM users WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return bool(row[0]) if row else False


async def set_auto_signals(user_id: int, enabled: bool) -> None:
    """Persist auto-signals preference for a user."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET auto_signals = ? WHERE user_id = ?",
            (1 if enabled else 0, user_id),
        )
        await db.commit()


async def load_all_auto_signals() -> dict[int, bool]:
    """Load auto_signals flag for all approved users (for startup warm-up)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, auto_signals FROM users WHERE status = 'approved'"
        ) as cursor:
            rows = await cursor.fetchall()
            return {row[0]: bool(row[1]) for row in rows}


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
    market_mode: str | None = None,
    used_tier: str | None = None,
    confidence_raw: float | None = None,
    confidence_band: str | None = None,
) -> int:
    """Save a new pending signal. Returns the row id."""
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO signal_outcomes
                (user_id, symbol, pair_label, direction, confidence,
                 strategy, expiration_sec, signal_price, outcome, created_at,
                 market_mode, used_tier, confidence_raw, confidence_band)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)
            """,
            (user_id, symbol, pair_label, direction, confidence,
             strategy, expiration_sec, signal_price, now,
             market_mode, used_tier, confidence_raw, confidence_band),
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


async def get_pair_stats(user_id: int, limit: int = 5) -> list[dict]:
    """Return win/loss breakdown by pair for a user, ordered by winrate desc."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT
                pair_label,
                COUNT(*) as total,
                SUM(CASE WHEN outcome = 'win'  THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) as losses
            FROM signal_outcomes
            WHERE user_id = ? AND outcome IN ('win', 'loss')
            GROUP BY pair_label
            ORDER BY wins DESC, total DESC
            LIMIT ?
            """,
            (user_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            result = []
            for r in rows:
                w, l = (r["wins"] or 0), (r["losses"] or 0)
                result.append({
                    "pair_label": r["pair_label"],
                    "total":      r["total"],
                    "wins":       w,
                    "losses":     l,
                    "winrate":    round(w / (w + l) * 100) if (w + l) > 0 else None,
                })
            return result


async def get_performance_report(days: int | None = None) -> list[dict]:
    """
    Admin report: win/loss grouped by strategy × market_mode × used_tier ×
    expiration × confidence_band.  Only rows with at least 1 resolved outcome.

    Args:
        days: if set, restrict to last N calendar days (UTC).
              If None, covers all time.

    Returns list of dicts, each with:
        strategy, market_mode, used_tier, expiration_sec,
        confidence_band, total, wins, losses, win_rate (0-100 float or None)
    """
    where = "outcome IN ('win', 'loss')"
    params: list = []
    if days:
        where += " AND created_at >= datetime('now', ?)"
        params.append(f"-{days} days")

    query = f"""
        SELECT
            COALESCE(strategy,        'unknown') AS strategy,
            COALESCE(market_mode,     'unknown') AS market_mode,
            COALESCE(used_tier,       'unknown') AS used_tier,
            expiration_sec,
            COALESCE(confidence_band, 'unknown') AS confidence_band,
            COUNT(*)                                              AS total,
            SUM(CASE WHEN outcome = 'win'  THEN 1 ELSE 0 END)   AS wins,
            SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END)   AS losses
        FROM signal_outcomes
        WHERE {where}
        GROUP BY strategy, market_mode, used_tier, expiration_sec, confidence_band
        ORDER BY strategy, market_mode, used_tier, expiration_sec, confidence_band
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()

    result = []
    for r in rows:
        w = r["wins"]  or 0
        l = r["losses"] or 0
        result.append({
            "strategy":        r["strategy"],
            "market_mode":     r["market_mode"],
            "used_tier":       r["used_tier"],
            "expiration_sec":  r["expiration_sec"],
            "confidence_band": r["confidence_band"],
            "total":           r["total"],
            "wins":            w,
            "losses":          l,
            "win_rate":        round(w / (w + l) * 100, 1) if (w + l) > 0 else None,
        })
    return result


async def get_pending_outcomes(max_age_sec: int = 1800) -> list[dict]:
    """
    Return pending signal outcomes created within the last max_age_sec seconds.
    Used at bot startup to recover tracking tasks lost due to restart.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT id, user_id, symbol, pair_label, direction, strategy,
                   expiration_sec, signal_price, created_at
            FROM signal_outcomes
            WHERE outcome = 'pending'
              AND created_at >= datetime('now', ?)
            ORDER BY created_at ASC
            """,
            (f"-{max_age_sec} seconds",),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_last_trades(limit: int = 10) -> list[dict]:
    """
    Return the most recent N signal outcomes across all pairs (all outcome states).
    Ordered newest first. Used for /debug no-args mode.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT
                id, symbol, pair_label, direction,
                confidence, confidence_raw, confidence_band,
                strategy, market_mode, used_tier,
                expiration_sec, signal_price, result_price,
                outcome, created_at, resolved_at
            FROM signal_outcomes
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_pair_outcomes(symbol: str, limit: int = 15) -> list[dict]:
    """
    Return recent resolved signal outcomes for a specific pair symbol.
    Matches symbol case-insensitively and with/without leading '#'.
    Ordered newest first. Used for /debug history section.
    """
    # Normalise: strip '#', lowercase, build LIKE pattern for both variants
    clean = symbol.lstrip("#").lower()         # e.g. "eurchf_otc"
    with_hash = "#" + clean                    # e.g. "#eurchf_otc"

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT
                direction, confidence, confidence_raw, strategy,
                market_mode, used_tier, expiration_sec,
                signal_price, result_price, outcome, created_at
            FROM signal_outcomes
            WHERE LOWER(symbol) IN (?, ?) AND outcome IN ('win', 'loss')
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (clean, with_hash, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_outcome_by_id(outcome_id: int) -> dict | None:
    """Return a single signal_outcomes row by id (all fields)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT id, direction, confidence, strategy, expiration_sec,
                   signal_price, result_price, outcome, created_at, resolved_at
            FROM signal_outcomes WHERE id = ?
            """,
            (outcome_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


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


# ── Condition frequency stats ────────────────────────────────────────────────────

async def record_condition_evals(evals: list[tuple[str, dict]]) -> None:
    """
    Record one round of strategy evaluations into condition_stats.

    evals: list of (strategy_name, conditions_dict)
           conditions_dict maps condition_name → bool  (non-bool values ignored)
    """
    if not evals:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        for strategy, conds in evals:
            # Increment strategy eval count
            await db.execute(
                """
                INSERT INTO strategy_evals (strategy, count) VALUES (?, 1)
                ON CONFLICT(strategy) DO UPDATE SET count = count + 1
                """,
                (strategy,),
            )
            # Increment per-condition true/false counts
            for cname, cval in conds.items():
                if not isinstance(cval, bool):
                    continue
                t_inc = 1 if cval else 0
                f_inc = 0 if cval else 1
                await db.execute(
                    """
                    INSERT INTO condition_stats (strategy, condition, true_count, false_count)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(strategy, condition) DO UPDATE SET
                        true_count  = true_count  + excluded.true_count,
                        false_count = false_count + excluded.false_count
                    """,
                    (strategy, cname, t_inc, f_inc),
                )
        await db.commit()


async def get_condition_stats() -> dict:
    """
    Returns {strategy: {"evaluated": N, "conditions": {cname: {"true": T, "false": F, "rate": pct}}}}
    ordered by strategy name, conditions ordered by pass rate ascending (worst first).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        async with db.execute("SELECT strategy, count FROM strategy_evals ORDER BY strategy") as cur:
            eval_rows = {r["strategy"]: r["count"] for r in await cur.fetchall()}

        async with db.execute(
            "SELECT strategy, condition, true_count, false_count FROM condition_stats ORDER BY strategy, condition"
        ) as cur:
            cond_rows = await cur.fetchall()

    result: dict = {}
    for strategy, count in eval_rows.items():
        result[strategy] = {"evaluated": count, "conditions": {}}

    for r in cond_rows:
        s = r["strategy"]
        if s not in result:
            result[s] = {"evaluated": 0, "conditions": {}}
        t = r["true_count"]
        f = r["false_count"]
        total = t + f
        rate = round(t / total * 100) if total > 0 else 0
        result[s]["conditions"][r["condition"]] = {"true": t, "false": f, "rate": rate}

    # Sort each strategy's conditions by pass rate ascending (bottlenecks first)
    for s in result:
        result[s]["conditions"] = dict(
            sorted(result[s]["conditions"].items(), key=lambda kv: kv[1]["rate"])
        )

    return result


async def reset_condition_stats() -> None:
    """Wipe all condition frequency data."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM strategy_evals")
        await db.execute("DELETE FROM condition_stats")
        await db.commit()


# ── Admin management ────────────────────────────────────────────────────────────

async def get_all_admin_ids() -> set[int]:
    """Return set of all assigned admin user_ids (excludes main admin from config)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT user_id FROM admins") as cursor:
            rows = await cursor.fetchall()
            return {r["user_id"] for r in rows}


async def get_all_admins() -> list[dict]:
    """Return full list of assigned admins with details."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, username, added_by, added_at FROM admins ORDER BY added_at"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def add_admin(user_id: int, username: str | None, added_by: int) -> bool:
    """Add an admin. Returns False if already exists."""
    now = datetime.utcnow().isoformat()
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO admins (user_id, username, added_by, added_at) VALUES (?, ?, ?, ?)",
                (user_id, username or "", added_by, now),
            )
            await db.commit()
        return True
    except aiosqlite.IntegrityError:
        return False


async def remove_admin(user_id: int) -> bool:
    """Remove an admin. Returns False if not found."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        await db.commit()
        return cursor.rowcount > 0


# ── Live strategy WR for adaptive confidence ──────────────────────────────────

async def get_recent_strategy_wr(
    strategy: str,
    n_trades: int = 40,
) -> tuple[float | None, int]:
    """
    Return (win_rate_pct, resolved_count) for the strategy based on the last
    `n_trades` resolved (win/loss) outcomes.

    Returns (None, 0) when fewer than 10 resolved trades exist (not enough data).
    Ignores 'pending' and 'draw'/'error' outcomes — counts only win+loss.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT outcome
            FROM signal_outcomes
            WHERE strategy = ?
              AND outcome IN ('win', 'loss')
            ORDER BY id DESC
            LIMIT ?
            """,
            (strategy, n_trades),
        ) as cursor:
            rows = await cursor.fetchall()

    if not rows:
        return None, 0

    total  = len(rows)
    wins   = sum(1 for r in rows if r[0] == 'win')
    wr_pct = round(wins / total * 100, 1)
    return wr_pct, total


async def get_all_strategies_live_wr(
    strategies: list[str],
    n_trades: int = 40,
    min_trades: int = 10,
) -> dict[str, float]:
    """
    Return live WR for all requested strategies in one call.
    Only includes strategies with at least `min_trades` resolved outcomes.
    """
    result: dict[str, float] = {}
    for strat in strategies:
        wr, count = await get_recent_strategy_wr(strat, n_trades)
        if wr is not None and count >= min_trades:
            result[strat] = wr
    return result
