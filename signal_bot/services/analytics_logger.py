"""
Analytics Logger — automatic signal and result tracking.

Captures every fired BUY/SELL signal with full engine debug context.
Updates WIN/LOSS when outcome is resolved.
Provides CSV export for offline analysis.

Tables:
  signals_log   — one row per fired signal, result filled on resolution
  candidates_log — one row per pattern candidate (detected + rejected)

Usage:
  # On signal fire:
  trade_id = await log_signal(outcome_id, pair, symbol, direction, expiry, entry_price, details)

  # On result resolution:
  await update_result(outcome_id, close_price, result, pnl_pct)

  # Admin export:
  n_rows = await export_csv("/tmp/signals_export.csv")
"""
from __future__ import annotations

import csv
import logging
import os
import sys
from datetime import datetime

import aiosqlite

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger(__name__)
_DB = config.DB_PATH


# ── Table DDL ─────────────────────────────────────────────────────────────────

_SIGNALS_LOG_DDL = """
CREATE TABLE IF NOT EXISTS signals_log (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    outcome_id             INTEGER,          -- FK → signal_outcomes.id (nullable if tracking off)
    timestamp_signal       TEXT NOT NULL,
    pair                   TEXT NOT NULL,
    symbol                 TEXT NOT NULL,
    direction              TEXT NOT NULL,    -- BUY | SELL
    expiry                 TEXT NOT NULL,    -- 1m | 2m
    entry_price            REAL,

    -- Engine output
    pattern_winner         TEXT,
    final_score            REAL,
    raw_pattern_score      REAL,
    direction_gap          REAL,
    regime                 TEXT,
    countertrend           INTEGER,          -- 0/1
    n_bars_15s             INTEGER,
    n_bars_1m              INTEGER,
    n_bars_5m              INTEGER,
    score_gap              REAL,             -- winner vs runner-up score gap
    filter_penalty         REAL,            -- total penalty applied to winner

    -- Level context
    nearest_support        REAL,
    dist_to_support_pct    REAL,
    nearest_resistance     REAL,
    dist_to_resistance_pct REAL,
    level_count_sup        INTEGER,
    level_count_res        INTEGER,

    -- Pattern-specific: impulse_pullback
    impulse_len            INTEGER,
    pullback_len           INTEGER,
    retracement_pct        REAL,
    confirm_ratio          REAL,

    -- Pattern-specific: level_rejection
    lr_level_price         REAL,
    lr_touches             INTEGER,
    lr_fresh               INTEGER,          -- 0/1
    lr_wick_ratio          REAL,
    lr_room_pct            REAL,

    -- Pattern-specific: false_breakout
    fb_level_price         REAL,
    fb_side                TEXT,
    fb_reclaim_type        TEXT,
    fb_bars_ago            INTEGER,

    -- Filter diagnostic fields
    no_room_decision       TEXT,             -- skip | hard | soft | n/a
    opposite_level_class   TEXT,             -- internal | external | ambiguous | n/a
    dead_market            INTEGER,          -- 0/1 (should always be 0 — dead market blocks signal)
    exhaustion             INTEGER,          -- 0/1
    noisy                  INTEGER,          -- 0/1
    range_penalty          INTEGER,          -- 0/1

    -- Result (filled by update_result)
    close_price            REAL,
    result                 TEXT DEFAULT 'pending',  -- WIN | LOSS | error | pending
    pnl_pct                REAL,
    resolved_at            TEXT
)
"""

_CANDIDATES_LOG_DDL = """
CREATE TABLE IF NOT EXISTS candidates_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    outcome_id     INTEGER,                  -- FK → signals_log.outcome_id
    pattern_name   TEXT NOT NULL,
    direction      TEXT NOT NULL,
    score          REAL,
    fit_for        TEXT,                     -- comma-separated
    status         TEXT NOT NULL,            -- winner | passed | rejected
    reject_reason  TEXT,
    logged_at      TEXT NOT NULL
)
"""


async def init_analytics() -> None:
    """Create analytics tables. Safe to call multiple times (idempotent)."""
    async with aiosqlite.connect(_DB) as db:
        await db.execute(_SIGNALS_LOG_DDL)
        await db.execute(_CANDIDATES_LOG_DDL)
        # Migrations: add columns that may not exist in older DBs
        _new_cols = [
            ("score_gap",              "REAL"),
            ("filter_penalty",         "REAL"),
            ("range_penalty",          "INTEGER"),
            ("fb_level_price",         "REAL"),
            ("fb_side",                "TEXT"),
            ("fb_reclaim_type",        "TEXT"),
            ("fb_bars_ago",            "INTEGER"),
        ]
        for col, dtype in _new_cols:
            try:
                await db.execute(
                    f"ALTER TABLE signals_log ADD COLUMN {col} {dtype}"
                )
            except Exception:
                pass
        await db.commit()
    logger.info("Analytics tables ready at %s", _DB)


# ── Signal logging ─────────────────────────────────────────────────────────────

async def log_signal(
    outcome_id: int | None,
    pair: str,
    symbol: str,
    direction: str,
    expiry: str,
    entry_price: float | None,
    details: dict,
) -> None:
    """
    Log a fired BUY/SELL signal to signals_log.
    Also logs all pattern candidates to candidates_log.

    Args:
        outcome_id:  Row ID from signal_outcomes table (used as FK / trade link).
        pair:        Human-readable pair label.
        symbol:      Raw symbol e.g. "#EURUSD_otc".
        direction:   "BUY" | "SELL".
        expiry:      "1m" | "2m".
        entry_price: Close price at signal time.
        details:     Full details dict from SignalResult (contains "debug" subkey).
    """
    try:
        d     = details if isinstance(details, dict) else {}
        dbg   = d.get("debug", {})
        pat   = dbg.get("pattern_detail", {})
        levels = dbg.get("levels", {})
        pdbg  = dbg.get("pattern_debug", {})

        pattern_winner = dbg.get("best_pattern") or d.get("primary_strategy")

        # direction_gap: prefer from pattern_detail (already extracted), then pattern_debug
        direction_gap = pat.get("direction_gap") or pdbg.get("direction_gap")

        # countertrend
        countertrend = pat.get("countertrend") or pdbg.get("countertrend")

        # Filter diagnostic fields
        filt_info = _parse_filter_reasons(
            dbg.get("filter_reasons", []) + dbg.get("filter_log", [])
        )

        # Pattern-specific fields
        ip_fields = _extract_ip_fields(pat, pattern_winner)
        lr_fields = _extract_lr_fields(pat, pattern_winner)
        fb_fields = _extract_fb_fields(pat, dbg.get("pattern_debug", {}), pattern_winner)

        now = datetime.utcnow().isoformat()

        async with aiosqlite.connect(_DB) as db:
            cursor = await db.execute(
                """
                INSERT INTO signals_log (
                    outcome_id, timestamp_signal, pair, symbol,
                    direction, expiry, entry_price,
                    pattern_winner, final_score, raw_pattern_score,
                    direction_gap, regime, countertrend,
                    n_bars_15s, n_bars_1m, n_bars_5m,
                    score_gap, filter_penalty,
                    nearest_support, dist_to_support_pct,
                    nearest_resistance, dist_to_resistance_pct,
                    level_count_sup, level_count_res,
                    impulse_len, pullback_len, retracement_pct, confirm_ratio,
                    lr_level_price, lr_touches, lr_fresh, lr_wick_ratio, lr_room_pct,
                    fb_level_price, fb_side, fb_reclaim_type, fb_bars_ago,
                    no_room_decision, opposite_level_class,
                    dead_market, exhaustion, noisy, range_penalty,
                    result
                ) VALUES (
                    ?,?,?,?,?,?,?,
                    ?,?,?,
                    ?,?,?,
                    ?,?,?,
                    ?,?,
                    ?,?,?,?,?,?,
                    ?,?,?,?,
                    ?,?,?,?,?,
                    ?,?,?,?,
                    ?,?,
                    ?,?,?,?,
                    'pending'
                )
                """,
                (
                    outcome_id,
                    now,
                    pair,
                    symbol,
                    direction,
                    expiry,
                    entry_price,
                    # engine
                    pattern_winner,
                    dbg.get("final_score"),
                    dbg.get("raw_score"),
                    direction_gap,
                    d.get("market_mode") or dbg.get("context", {}).get("regime"),
                    1 if countertrend else 0,
                    dbg.get("n_15s"),
                    dbg.get("n_1m"),
                    dbg.get("n_5m"),
                    dbg.get("score_gap"),
                    dbg.get("filter_penalty"),
                    # levels
                    levels.get("nearest_sup"),
                    levels.get("dist_sup_pct"),
                    levels.get("nearest_res"),
                    levels.get("dist_res_pct"),
                    levels.get("n_supports"),
                    levels.get("n_resistances"),
                    # ip
                    ip_fields["impulse_len"],
                    ip_fields["pullback_len"],
                    ip_fields["retracement_pct"],
                    ip_fields["confirm_ratio"],
                    # lr
                    lr_fields["lr_level_price"],
                    lr_fields["lr_touches"],
                    lr_fields["lr_fresh"],
                    lr_fields["lr_wick_ratio"],
                    lr_fields["lr_room_pct"],
                    # fb
                    fb_fields["fb_level_price"],
                    fb_fields["fb_side"],
                    fb_fields["fb_reclaim_type"],
                    fb_fields["fb_bars_ago"],
                    # filters
                    filt_info["no_room_decision"],
                    filt_info["opposite_level_class"],
                    filt_info["dead_market"],
                    filt_info["exhaustion"],
                    filt_info["noisy"],
                    filt_info["range_penalty"],
                ),
            )
            signals_log_id = cursor.lastrowid
            await db.commit()

        logger.debug(
            "Analytics: signal logged id=%d outcome_id=%s %s %s %s score=%.1f",
            signals_log_id or 0, outcome_id, pair, direction, expiry,
            dbg.get("final_score") or 0,
        )

        # ── Candidates log ────────────────────────────────────────────────────
        await _log_candidates(outcome_id, pattern_winner, direction, dbg)

    except Exception as exc:
        logger.warning("analytics_logger.log_signal failed: %s", exc, exc_info=False)


async def update_result(
    outcome_id: int,
    close_price: float,
    result: str,
    pnl_pct: float | None = None,
) -> None:
    """
    Update trade result in signals_log when outcome is resolved.

    Args:
        outcome_id:  Same ID used when log_signal was called.
        close_price: Closing price at expiry.
        result:      "WIN" | "LOSS" | "error"
        pnl_pct:     Percentage price move (positive = direction correct).
    """
    try:
        now = datetime.utcnow().isoformat()
        result_norm = result.upper() if result in ("win", "loss") else result

        async with aiosqlite.connect(_DB) as db:
            await db.execute(
                """
                UPDATE signals_log
                SET close_price = ?, result = ?, pnl_pct = ?, resolved_at = ?
                WHERE outcome_id = ?
                """,
                (close_price, result_norm, pnl_pct, now, outcome_id),
            )
            await db.commit()

        logger.debug(
            "Analytics: result updated outcome_id=%d → %s close=%.6f pnl=%.4f%%",
            outcome_id, result_norm, close_price, pnl_pct or 0,
        )

    except Exception as exc:
        logger.warning("analytics_logger.update_result failed: %s", exc, exc_info=False)


# ── CSV export ────────────────────────────────────────────────────────────────

async def export_csv(filepath: str) -> int:
    """
    Export signals_log to CSV file.
    Returns number of rows exported.
    """
    try:
        async with aiosqlite.connect(_DB) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM signals_log ORDER BY id DESC"
            ) as cur:
                rows = await cur.fetchall()

        if not rows:
            return 0

        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row))

        logger.info("Analytics: exported %d rows to %s", len(rows), filepath)
        return len(rows)

    except Exception as exc:
        logger.warning("analytics_logger.export_csv failed: %s", exc)
        return 0


async def get_summary() -> dict:
    """Return quick summary stats for admin display."""
    try:
        async with aiosqlite.connect(_DB) as db:
            db.row_factory = aiosqlite.Row

            async with db.execute(
                "SELECT COUNT(*) as total FROM signals_log"
            ) as cur:
                total = (await cur.fetchone())["total"]

            async with db.execute(
                "SELECT COUNT(*) as n FROM signals_log WHERE result='WIN'"
            ) as cur:
                wins = (await cur.fetchone())["n"]

            async with db.execute(
                "SELECT COUNT(*) as n FROM signals_log WHERE result='LOSS'"
            ) as cur:
                losses = (await cur.fetchone())["n"]

            async with db.execute(
                """SELECT pattern_winner, COUNT(*) as n,
                          SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w
                   FROM signals_log WHERE result IN ('WIN','LOSS')
                   GROUP BY pattern_winner ORDER BY n DESC"""
            ) as cur:
                by_pattern = [dict(r) for r in await cur.fetchall()]

            async with db.execute(
                """SELECT expiry, COUNT(*) as n,
                          SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as w
                   FROM signals_log WHERE result IN ('WIN','LOSS')
                   GROUP BY expiry"""
            ) as cur:
                by_expiry = [dict(r) for r in await cur.fetchall()]

        resolved = wins + losses
        wr = round(wins / resolved * 100, 1) if resolved else None
        return {
            "total":      total,
            "resolved":   resolved,
            "wins":       wins,
            "losses":     losses,
            "winrate":    wr,
            "by_pattern": by_pattern,
            "by_expiry":  by_expiry,
        }
    except Exception as exc:
        logger.warning("analytics_logger.get_summary failed: %s", exc)
        return {}


# ── Internal helpers ──────────────────────────────────────────────────────────

def _parse_filter_reasons(reasons: list[str]) -> dict:
    """
    Parse filter reason strings into structured flags.
    Strings come from global_filters_v2 and decision_engine_v2 guard sections.
    """
    no_room_decision    = "n/a"
    opposite_level_class = "n/a"
    dead_market  = 0
    exhaustion   = 0
    noisy        = 0
    range_penalty = 0

    for r in reasons:
        if not isinstance(r, str):
            continue
        rl = r.lower()

        # IP no_room classification
        if "ip_internal_swing" in rl:
            no_room_decision     = "skip"
            opposite_level_class = "internal"
        elif "ip_no_room_" in rl:
            no_room_decision     = "hard"
            opposite_level_class = "external"
        elif "ip_close_level_" in rl:
            if no_room_decision == "n/a":  # soft beats n/a, don't override hard
                no_room_decision     = "soft"
                opposite_level_class = "ambiguous"

        # Global hard filter flags
        if "dead_market" in rl:
            dead_market = 1
        if "exhaustion_" in rl:
            exhaustion = 1
        if "noisy:" in rl:
            noisy = 1
        if "range_penalty" in rl:
            range_penalty = 1

    return {
        "no_room_decision":    no_room_decision,
        "opposite_level_class": opposite_level_class,
        "dead_market":  dead_market,
        "exhaustion":   exhaustion,
        "noisy":        noisy,
        "range_penalty": range_penalty,
    }


def _extract_ip_fields(pat: dict, pattern_winner: str | None) -> dict:
    empty = {"impulse_len": None, "pullback_len": None,
             "retracement_pct": None, "confirm_ratio": None}
    if pattern_winner != "impulse_pullback":
        return empty
    return {
        "impulse_len":     pat.get("imp_bars"),
        "pullback_len":    pat.get("pb_bars"),
        "retracement_pct": pat.get("retracement_pct"),
        "confirm_ratio":   pat.get("conf_body_ratio"),
    }


def _extract_lr_fields(pat: dict, pattern_winner: str | None) -> dict:
    empty = {"lr_level_price": None, "lr_touches": None, "lr_fresh": None,
             "lr_wick_ratio": None, "lr_room_pct": None}
    if pattern_winner != "level_rejection":
        return empty
    return {
        "lr_level_price": pat.get("level_price"),
        "lr_touches":     pat.get("level_touches"),
        "lr_fresh":       1 if pat.get("level_fresh") else 0 if pat.get("level_fresh") is not None else None,
        "lr_wick_ratio":  pat.get("wick_ratio"),
        "lr_room_pct":    pat.get("room_pct"),
    }


def _extract_fb_fields(pat: dict, pdbg: dict, pattern_winner: str | None) -> dict:
    empty = {"fb_level_price": None, "fb_side": None,
             "fb_reclaim_type": None, "fb_bars_ago": None}
    if pattern_winner != "false_breakout":
        return empty
    # FB debug is flatter — try to get from explanation text or debug dict
    explanation = pdbg.get("explanation", "") or ""
    # Extract side (up/down) from breakout_side field if available
    return {
        "fb_level_price": None,           # FB doesn't surface level_price directly
        "fb_side":        pdbg.get("breakout_side"),
        "fb_reclaim_type": pdbg.get("reclaim_type") or pdbg.get("tolerance_pct"),
        "fb_bars_ago":    None,            # buried in explanation text
    }


async def _log_candidates(
    outcome_id: int | None,
    winner_pattern: str | None,
    winner_direction: str,
    dbg: dict,
) -> None:
    """Log all pattern candidates to candidates_log."""
    try:
        all_patterns  = dbg.get("all_patterns", [])    # passed all filters
        filter_log    = dbg.get("filter_log", [])       # rejected reasons (strings)
        now = datetime.utcnow().isoformat()

        rows: list[tuple] = []

        # Passed candidates
        for p in all_patterns:
            name = p.get("name", "")
            direction = p.get("direction", "")
            score = p.get("score", 0.0)
            fit   = ",".join(p.get("fit_for", []))
            is_winner = (name == winner_pattern and direction == winner_direction)
            status = "winner" if is_winner else "passed"
            rows.append((outcome_id, name, direction, score, fit, status, None, now))

        # Parse filter_log for rejected patterns (format: "pattern dir REJECTED by filter: reason")
        for msg in filter_log:
            if not isinstance(msg, str):
                continue
            parts = msg.split(" ", 3)
            if len(parts) >= 2:
                name = parts[0]
                direction = parts[1] if parts[1] in ("BUY", "SELL") else "?"
                reason = msg[len(f"{name} {direction} "):].strip() if len(parts) >= 3 else msg
                rows.append((outcome_id, name, direction, None, None, "rejected", reason[:200], now))

        if not rows:
            return

        async with aiosqlite.connect(_DB) as db:
            await db.executemany(
                """INSERT INTO candidates_log
                   (outcome_id, pattern_name, direction, score, fit_for, status, reject_reason, logged_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                rows,
            )
            await db.commit()

    except Exception as exc:
        logger.debug("_log_candidates failed: %s", exc)
