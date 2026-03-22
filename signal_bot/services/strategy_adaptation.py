"""
Strategy Adaptation Module

Silently tracks each strategy's real performance and adjusts their usage.
User sees nothing — they simply receive better signals over time.

Statuses:
  ACTIVE     — works normally, multiplier x1.0
  WEAKENED   — confidence x0.85 (only strong signals pass threshold)
  PROBATION  — re-enabled after WEAKENED run, confidence x0.80;
               evaluated after 5 new results accumulate

Note: No strategy is ever fully DISABLED. A low-winrate strategy naturally
produces low confidence, which falls below the threshold — no signal shown.
Auto-disabling is redundant and harmful: it removes the strategy from debug
output and prevents it from recovering without artificial delay.
"""
from __future__ import annotations

import logging
import time

import aiosqlite

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

ALL_STRATEGIES = [
    "ema_bounce",
    "squeeze_breakout",
    "level_bounce",
    "rsi_reversal",
    "micro_breakout",
    "divergence",
]

_MULTIPLIERS = {
    "ACTIVE":    1.0,
    "WEAKENED":  0.85,
    "PROBATION": 0.80,
}

_CACHE_TTL        = 60.0   # seconds between full DB scans
_MIN_DATA         = 8      # minimum results before evaluating status
_SAMPLE_SIZE      = 20     # last N results used for evaluation
_PROBATION_SAMPLE = 5      # results needed in probation to re-evaluate

# ── In-memory state ────────────────────────────────────────────────────────────

_status: dict[str, dict] = {
    name: {
        "status":                "ACTIVE",
        "winrate":               None,
        "total_signals":         0,
        "confidence_multiplier": 1.0,
        "probation_start":       None,   # epoch when probation began
    }
    for name in ALL_STRATEGIES
}

_last_update: float = 0.0
_DB_PATH: str | None = None


def _db_path() -> str:
    global _DB_PATH
    if _DB_PATH is None:
        try:
            import config as _cfg
            _DB_PATH = _cfg.DB_PATH
        except Exception:
            import os
            _DB_PATH = os.path.join(os.path.dirname(__file__), "..", "signal_bot.db")
    return _DB_PATH


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _fetch_outcomes(name: str, limit: int, after_epoch: float | None = None) -> list[str]:
    """Return recent outcomes ('win'/'loss') for a strategy, newest first."""
    try:
        if after_epoch is not None:
            from datetime import datetime, timezone
            after_iso = datetime.fromtimestamp(after_epoch, tz=timezone.utc).isoformat()
            query = """
                SELECT outcome FROM signal_outcomes
                WHERE strategy = ? AND outcome IN ('win', 'loss')
                  AND created_at >= ?
                ORDER BY created_at DESC
                LIMIT ?
            """
            params = (name, after_iso, limit)
        else:
            query = """
                SELECT outcome FROM signal_outcomes
                WHERE strategy = ? AND outcome IN ('win', 'loss')
                ORDER BY created_at DESC
                LIMIT ?
            """
            params = (name, limit)

        async with aiosqlite.connect(_db_path()) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [r["outcome"] for r in rows]
    except Exception as e:
        logger.warning("strategy_adaptation: DB query failed for %s: %s", name, e)
        return []


def _set_status(name: str, new_status: str, winrate: float | None, total: int) -> None:
    """Apply a new status to a strategy entry."""
    entry = _status[name]
    prev  = entry["status"]
    entry["status"]                = new_status
    entry["confidence_multiplier"] = _MULTIPLIERS[new_status]
    entry["winrate"]               = round(winrate * 100, 1) if winrate is not None else None
    entry["total_signals"]         = total

    if new_status == "PROBATION" and prev != "PROBATION":
        entry["probation_start"] = time.time()
    elif new_status != "PROBATION":
        entry["probation_start"] = None

    if prev != new_status:
        logger.info(
            "Strategy %s: %s → %s (winrate=%.0f%%, n=%d)",
            name, prev, new_status, (winrate or 0) * 100, total
        )


# ── Public API ─────────────────────────────────────────────────────────────────

async def update_strategy_statuses() -> None:
    """
    Refresh all strategy statuses from DB. Cached for 60 s.
    Call at the start of calculate_signal() — returns instantly when cache is hot.
    """
    global _last_update
    now = time.time()
    if now - _last_update < _CACHE_TTL:
        return
    _last_update = now

    for name in ALL_STRATEGIES:
        entry = _status[name]

        # ── PROBATION: check if 5 new results have accumulated ─────────────────
        if entry["status"] == "PROBATION":
            probation_start = entry.get("probation_start") or now
            results = await _fetch_outcomes(name, _PROBATION_SAMPLE, after_epoch=probation_start)
            total   = len(results)
            if total >= _PROBATION_SAMPLE:
                wins    = results.count("win")
                winrate = wins / total
                if winrate >= 0.60:
                    new_s = "ACTIVE"
                else:
                    new_s = "WEAKENED"
                _set_status(name, new_s, winrate, total)
                logger.info(
                    "Strategy %s probation ended: %d/%d wins → %s",
                    name, wins, total, new_s
                )
            continue   # don't overwrite probation with normal eval

        # ── Normal evaluation ──────────────────────────────────────────────────
        results = await _fetch_outcomes(name, _SAMPLE_SIZE)
        total   = len(results)

        if total < _MIN_DATA:
            # Not enough data yet — stay ACTIVE (safe default)
            if entry["status"] != "ACTIVE":
                _set_status(name, "ACTIVE", None, total)
            continue

        wins    = results.count("win")
        winrate = wins / total

        if winrate >= 0.50:
            target = "ACTIVE"
        elif winrate >= 0.38:
            target = "WEAKENED"
        else:
            # Poor winrate → PROBATION (multiplier 0.80, re-evaluated after 5 results)
            # Strategy stays visible and keeps running — confidence naturally drops
            target = "PROBATION"

        _set_status(name, target, winrate, total)


def get_confidence_multiplier(strategy_name: str) -> float:
    """Return confidence multiplier for strategy (1.0 / 0.85 / 0.80)."""
    return _status.get(strategy_name, {}).get("confidence_multiplier", 1.0)


def is_strategy_enabled(strategy_name: str) -> bool:
    """All strategies are always enabled. Kept for API compatibility."""
    return True


async def initialize() -> None:
    """
    Call at bot startup. Forces an immediate status refresh if data exists.
    Falls back to all-ACTIVE if no data yet — safe default.
    """
    global _last_update
    _last_update = 0.0  # force refresh on first calculate_signal() call
    logger.info("strategy_adaptation: ready — all strategies start as ACTIVE")
