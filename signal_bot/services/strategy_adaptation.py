"""
Strategy Adaptation Module

Silently tracks each strategy's real performance and adjusts their usage.
User sees nothing — they simply receive better signals over time.

Statuses:
  ACTIVE     — works normally, multiplier x1.0
  WEAKENED   — confidence x0.85 (only strong signals pass threshold)
  DISABLED   — completely skipped for 20 minutes
  PROBATION  — re-enabled after 20 min, confidence x0.80;
               evaluated after 5 new results accumulate
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

# Strategies that can never be fully DISABLED — worst case is WEAKENED.
_PROTECTED_STRATEGIES: set[str] = {"ema_bounce"}

_MULTIPLIERS = {
    "ACTIVE":    1.0,
    "WEAKENED":  0.85,
    "PROBATION": 0.80,
    "DISABLED":  0.0,
}

_CACHE_TTL        = 60.0        # seconds between full DB scans
_PROBATION_DELAY  = 20 * 60     # seconds until DISABLED → PROBATION
_MIN_DATA         = 8           # minimum results before evaluating status
_SAMPLE_SIZE      = 20          # last N results used for evaluation
_PROBATION_SAMPLE = 5           # results needed in probation to re-evaluate

# ── In-memory state ────────────────────────────────────────────────────────────

_status: dict[str, dict] = {
    name: {
        "status":               "ACTIVE",
        "winrate":              None,
        "total_signals":        0,
        "confidence_multiplier": 1.0,
        "disabled_at":          None,   # epoch when strategy was disabled
        "probation_start":      None,   # epoch when probation began
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

def _count_usable() -> int:
    """Count ACTIVE + WEAKENED + PROBATION strategies."""
    return sum(1 for s in _status.values() if s["status"] != "DISABLED")


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
    entry  = _status[name]
    prev   = entry["status"]
    entry["status"]               = new_status
    entry["confidence_multiplier"] = _MULTIPLIERS[new_status]
    entry["winrate"]               = round(winrate * 100, 1) if winrate is not None else None
    entry["total_signals"]         = total

    if new_status == "DISABLED" and prev != "DISABLED":
        entry["disabled_at"]     = time.time()
        entry["probation_start"] = None
        logger.info("Strategy %s → DISABLED (winrate=%.0f%%, n=%d)", name,
                    (winrate or 0) * 100, total)
    elif new_status == "PROBATION" and prev == "DISABLED":
        entry["probation_start"] = time.time()
        entry["disabled_at"]     = None
        logger.info("Strategy %s: DISABLED → PROBATION (20 min elapsed)", name)
    elif prev != new_status:
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

        # ── DISABLED → PROBATION after 20 min ─────────────────────────────────
        if entry["status"] == "DISABLED":
            disabled_since = entry.get("disabled_at") or 0.0
            if now - disabled_since >= _PROBATION_DELAY:
                _set_status(name, "PROBATION", None, 0)
            continue   # don't re-evaluate from DB while disabled

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
                elif winrate >= 0.40:
                    new_s = "WEAKENED"
                else:
                    new_s = "DISABLED"
                # Protected strategies can never be fully disabled
                if new_s == "DISABLED" and name in _PROTECTED_STRATEGIES:
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
            # Safety floor: always keep at least 2 usable strategies
            if _count_usable() <= 2 and entry["status"] in ("ACTIVE", "WEAKENED"):
                target = "WEAKENED"
                logger.info(
                    "Strategy %s winrate=%.0f%% — safety floor, setting WEAKENED not DISABLED",
                    name, winrate * 100
                )
            else:
                target = "DISABLED"

        # Protected strategies can never be fully disabled — floor is WEAKENED
        if target == "DISABLED" and name in _PROTECTED_STRATEGIES:
            target = "WEAKENED"
            logger.info(
                "Strategy %s winrate=%.0f%% — protected, setting WEAKENED not DISABLED",
                name, winrate * 100
            )

        _set_status(name, target, winrate, total)


def get_confidence_multiplier(strategy_name: str) -> float:
    """Return confidence multiplier for strategy (1.0 / 0.85 / 0.80 / 0.0)."""
    return _status.get(strategy_name, {}).get("confidence_multiplier", 1.0)


def is_strategy_enabled(strategy_name: str) -> bool:
    """Return False only when strategy status is DISABLED."""
    return _status.get(strategy_name, {}).get("status") != "DISABLED"


async def initialize() -> None:
    """
    Call at bot startup. Forces an immediate status refresh if data exists.
    Falls back to all-ACTIVE if no data yet — safe default.
    """
    global _last_update
    _last_update = 0.0  # force refresh on first calculate_signal() call
    logger.info("strategy_adaptation: ready — all strategies start as ACTIVE")
