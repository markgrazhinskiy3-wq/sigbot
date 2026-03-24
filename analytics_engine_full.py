"""
Candle Cache — scalable data layer for 100+ simultaneous users.

Architecture:
  - One background refresher loops through ALL OTC pairs every REFRESH_INTERVAL seconds.
  - It fetches candles using the browser (one pair at a time, respecting the lock).
  - User signal requests are served instantly from this cache — no browser open per user.
  - On a cache miss (pair never loaded yet), the request falls through to a live fetch.

Result: 100+ users can request signals simultaneously; all see responses in <1 second
        after the first warm-up cycle (~REFRESH_INTERVAL seconds after startup).
"""

import asyncio
import logging
import time
from typing import NamedTuple

import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger(__name__)

# How long a cached entry stays "fresh" (seconds)
# Must be significantly larger than REFRESH_INTERVAL to survive slow refresh cycles.
CACHE_TTL: int = 90

# How often the background loop refreshes all pairs (should be << CACHE_TTL)
REFRESH_INTERVAL: int = 45

# How many candles to keep per pair.
# At 15s per candle and 45s refresh interval, each pair grows by ~3 candles/cycle.
# 200 bars ≈ 50 min of history at steady state (~37 min accumulation after startup).
CANDLE_COUNT: int = 200

# Minimum minutes of accumulation before signals are allowed.
# After startup the cache has ~55 bars (13 min); we wait until enough WS cycles
# have run to build a meaningful history for indicator quality.
DATA_READY_MINUTES: int = 2


class _CacheEntry(NamedTuple):
    candles: list[dict]
    fetched_at: float  # unix timestamp


_cache: dict[str, _CacheEntry] = {}
_refresher_task: asyncio.Task | None = None
_warm_up_done: bool = False
_bot_start_time: float = time.time()


def is_warm_up_done() -> bool:
    """Returns True once the initial browser warm-up cycle has completed."""
    return _warm_up_done


def data_ready_in_seconds() -> int:
    """Seconds remaining until data accumulation period is over. 0 = ready."""
    elapsed = time.time() - _bot_start_time
    remaining = DATA_READY_MINUTES * 60 - elapsed
    return max(0, int(remaining))


def is_data_ready() -> bool:
    """
    True when both:
      1. Initial browser warm-up has finished (all pairs fetched at least once).
      2. DATA_READY_MINUTES have passed since startup (cache has accumulated history).
    """
    return _warm_up_done and data_ready_in_seconds() == 0


def get_cached_symbols() -> list[str]:
    """Return all symbols currently present in the cache (fresh or not)."""
    return list(_cache.keys())


def get_cached(symbol: str) -> list[dict] | None:
    """
    Return cached candles for symbol if they are still fresh, else None.
    This is the fast path — no I/O, no locking, safe to call from any coroutine.
    """
    entry = _cache.get(symbol)
    if entry and (time.time() - entry.fetched_at) < CACHE_TTL:
        age = round(time.time() - entry.fetched_at, 1)
        logger.debug(
            "Cache hit: %s (age=%.1fs, %d candles)", symbol, age, len(entry.candles)
        )
        return list(entry.candles)  # return a copy so callers can't mutate cache
    return None


def store(symbol: str, candles: list[dict]) -> None:
    """Full replace: store freshly-fetched candles in the cache."""
    trimmed = candles[-CANDLE_COUNT:] if len(candles) > CANDLE_COUNT else candles
    _cache[symbol] = _CacheEntry(candles=list(trimmed), fetched_at=time.time())
    logger.debug("Cache stored: %s (%d candles)", symbol, len(trimmed))


def store_merge(symbol: str, new_candles: list[dict]) -> bool:
    """
    Merge `new_candles` into the existing cache entry (by `time` key).
    New candles that overlap with the cache are updated in-place;
    newer candles are appended; older history from cache is preserved.
    Falls back to full replace if either side lacks timestamps.
    Returns True if cache was updated, False if new_candles was empty.
    """
    if not new_candles:
        return False

    # Filter out candles with zero/missing timestamps before merge
    new_with_time = [c for c in new_candles if c.get("time", 0) > 0]
    if not new_with_time:
        store(symbol, new_candles)
        return True

    existing = _cache.get(symbol)
    if not existing or not existing.candles:
        store(symbol, new_candles)
        return True

    old_with_time = [c for c in existing.candles if c.get("time", 0) > 0]
    if not old_with_time:
        # Existing cache has no timestamps — full replace with new data
        store(symbol, new_candles)
        return True

    # Merge by time: existing + new, deduplicated, newest wins for overlap
    by_time: dict[int, dict] = {c["time"]: c for c in old_with_time}
    for c in new_with_time:
        by_time[c["time"]] = c  # new data takes precedence

    merged = sorted(by_time.values(), key=lambda c: c["time"])
    trimmed = merged[-CANDLE_COUNT:] if len(merged) > CANDLE_COUNT else merged
    _cache[symbol] = _CacheEntry(candles=trimmed, fetched_at=time.time())
    logger.debug(
        "Cache merged: %s (%d existing + %d new = %d total)",
        symbol,
        len(existing.candles),
        len(new_candles),
        len(trimmed),
    )
    return True


async def _refresh_all_via_ws(symbols: list[str]) -> tuple[int, list[str]]:
    """
    Fetch candles for all symbols in one WebSocket connection.
    Merges new candles with existing cache (preserves history).
    Returns (ok_count, fallback_needed) where fallback_needed are symbols
    that got NO candles from WS (need browser fallback).
    """
    from services.po_ws_client import fetch_all_pairs, is_available

    if not is_available():
        return 0, list(symbols)

    try:
        async with asyncio.timeout(60):
            results = await fetch_all_pairs(symbols)
        ok = 0
        fallback = []
        for symbol in symbols:
            candles = results.get(symbol, [])
            if candles:
                store_merge(symbol, candles)
                ok += 1
            else:
                # No WS data — need browser if cache is also empty/stale
                entry = _cache.get(symbol)
                if not entry:
                    fallback.append(symbol)
        return ok, fallback
    except TimeoutError:
        logger.error("WS refresh timed out after 60s")
        return 0, list(symbols)
    except Exception as e:
        logger.error("WS refresh failed: %s", e)
        return 0, list(symbols)


async def _refresh_one_browser(symbol: str) -> bool:
    """Fallback: fetch candles via browser (used until WS auth is captured)."""
    from services.pocket_browser import _get_candles_impl, _get_lock

    try:
        async with asyncio.timeout(90):
            async with _get_lock():
                candles = await _get_candles_impl(symbol, CANDLE_COUNT)
        if candles:
            store(symbol, candles)
            return True
        logger.warning("Browser cache refresh returned empty candles for %s", symbol)
        return False
    except TimeoutError:
        logger.error("Browser cache refresh timed out for %s", symbol)
        return False
    except Exception as e:
        logger.error("Browser cache refresh failed for %s: %s", symbol, e)
        return False


async def _refresher_loop(pairs: list[str]) -> None:
    """
    Continuously refresh all pairs.
    - Phase 1 (startup): browser-based, one pair at a time (until WS auth captured)
    - Phase 2 (steady-state): direct WebSocket, all pairs in one connection (~5 sec/cycle)
    """
    from services.po_ws_client import is_available as ws_available

    logger.info(
        "Candle cache refresher started — %d pairs, TTL=%ds, interval=%ds",
        len(pairs),
        CACHE_TTL,
        REFRESH_INTERVAL,
    )

    # Initial warm-up:
    # If WS auth is already available → use WS directly (fast, 40-55 candles each).
    # Otherwise → try browser, fall back to WS when browser fails (ETXTBSY on Railway).
    MIN_READY_PAIRS = min(10, len(pairs))
    global _warm_up_done

    if ws_available():
        logger.info("Initial cache warm-up via WS (%d pairs)...", len(pairs))
        try:
            ws_results = await _refresh_all_via_ws(pairs)
            loaded = ws_results[0]  # ok count
        except Exception as e:
            logger.warning("WS warm-up failed: %s", e)
            loaded = 0
    else:
        logger.info(
            "Initial cache warm-up (browser, %d pairs, ready after %d)...",
            len(pairs),
            MIN_READY_PAIRS,
        )
        loaded = 0
        for symbol in pairs:
            ok = await _refresh_one_browser(symbol)
            if not ok and ws_available():
                # Browser failed (ETXTBSY during deployment) — try WS fallback
                ws_ok, _ = await _refresh_all_via_ws([symbol])
                ok = ws_ok > 0
            if ok:
                loaded += 1
                entry = _cache.get(symbol)
                logger.info(
                    "Warm-up: %s cached (%d candles) [%d/%d]",
                    symbol,
                    len(entry.candles) if entry else 0,
                    loaded,
                    len(pairs),
                )
                if not _warm_up_done and loaded >= MIN_READY_PAIRS:
                    _warm_up_done = True
                    logger.info(
                        "Cache warm-up threshold reached (%d pairs). Bot is now ready.",
                        loaded,
                    )

    if not _warm_up_done:
        _warm_up_done = True
    logger.info(
        "Cache warm-up complete (%d/%d pairs). WS auth available: %s",
        loaded,
        len(pairs),
        ws_available(),
    )

    while True:
        await asyncio.sleep(REFRESH_INTERVAL)
        cycle_start = time.time()

        if ws_available():
            ok, fallback = await _refresh_all_via_ws(pairs)
            # Browser fallback for pairs with insufficient candles from WS
            for sym in fallback:
                if await _refresh_one_browser(sym):
                    ok += 1
            elapsed = round(time.time() - cycle_start, 1)
            logger.info(
                "Cache refresh: %d/%d pairs (WS+fallback) in %.1fs",
                ok,
                len(pairs),
                elapsed,
            )
        else:
            ok = 0
            for symbol in pairs:
                if await _refresh_one_browser(symbol):
                    ok += 1
            elapsed = round(time.time() - cycle_start, 1)
            logger.info(
                "Browser cache refresh: %d/%d pairs in %.1fs", ok, len(pairs), elapsed
            )


def start_refresher(pairs: list[dict] | None = None) -> None:
    """
    Start the background cache refresher.
    `pairs` — list of {"symbol": ..., "label": ...} dicts (defaults to config.OTC_PAIRS).
    Call once from main() after the event loop is running.
    """
    global _refresher_task
    if _refresher_task is not None and not _refresher_task.done():
        logger.warning("Cache refresher already running, skipping start.")
        return

    pair_list = pairs or config.OTC_PAIRS
    symbols = [p["symbol"] for p in pair_list]
    _refresher_task = asyncio.create_task(_refresher_loop(symbols))
    logger.info("Cache refresher task created for %d pairs.", len(symbols))


async def refresh_pair_now(symbol: str) -> list[dict]:
    """
    Force-refresh candles for ONE symbol right now (called before a user signal).
    - WS available: fetch fresh candles via WS, merge into cache, return full history.
    - WS not available: return whatever is in cache (refresher keeps it ≤90s old).
    Does NOT compete with the browser — uses WS only.
    """
    from services.po_ws_client import fetch_all_pairs, is_available

    if is_available():
        try:
            async with asyncio.timeout(20):
                results = await fetch_all_pairs([symbol])
            candles = results.get(symbol, [])
            if candles:
                store_merge(symbol, candles)
                entry = _cache.get(symbol)
                if entry:
                    logger.info(
                        "Signal refresh via WS: %s (%d candles)",
                        symbol,
                        len(entry.candles),
                    )
                    return list(entry.candles)
        except Exception as e:
            logger.warning(
                "WS signal-refresh failed for %s: %s — using cache", symbol, e
            )

    # Fallback: use whatever is in the cache
    cached = get_cached(symbol)
    if cached:
        return cached
    return []


def resample_to_1m(candles_15s: list[dict]) -> list[dict]:
    """
    Resample 15-second candles into 1-minute candles.

    Primary:  group by Unix timestamp bucket (time // 60 * 60).
    Fallback: if fewer than 5 bars produced (candles lack valid timestamps),
              group sequentially — every 4 consecutive 15s bars = 1 minute.
              Assigns synthetic monotonic timestamps (0, 60, 120, …) so that
              resample_to_5m() can further aggregate correctly.
    Returns list sorted oldest → newest.
    """
    from collections import defaultdict

    groups: dict[int, list[dict]] = defaultdict(list)
    for c in candles_15s:
        t = c.get("time", 0)
        if t <= 0:
            continue
        bucket = (t // 60) * 60
        groups[bucket].append(c)

    result = []
    for bucket in sorted(groups.keys()):
        g = groups[bucket]
        result.append(
            {
                "time": bucket,
                "open": g[0]["open"],
                "high": max(c["high"] for c in g),
                "low": min(c["low"] for c in g),
                "close": g[-1]["close"],
            }
        )

    # Fallback: timestamps were missing/zero — aggregate sequentially (4×15s = 1m)
    if len(result) < 5 and candles_15s:
        # Filter candles that have all OHLC fields present and non-zero
        valid = [
            c
            for c in candles_15s
            if c.get("open") and c.get("high") and c.get("low") and c.get("close")
        ]
        if not valid:
            return result  # nothing usable
        result = []
        i = 0
        bucket_idx = 0
        while i < len(valid):
            g = valid[i : i + 4]
            result.append(
                {
                    "time": bucket_idx * 60,  # synthetic monotonic timestamp
                    "open": float(g[0]["open"]),
                    "high": float(max(c["high"] for c in g)),
                    "low": float(min(c["low"] for c in g)),
                    "close": float(g[-1]["close"]),
                }
            )
            i += 4
            bucket_idx += 1

    return result


def resample_to_5m(candles_1m: list[dict]) -> list[dict]:
    """
    Resample 1-minute candles into 5-minute candles.
    Groups by 5-minute bucket (time // 300 * 300).
    Works correctly with both real and synthetic timestamps from resample_to_1m().
    Returns list sorted oldest → newest.
    """
    from collections import defaultdict

    groups: dict[int, list[dict]] = defaultdict(list)
    for c in candles_1m:
        t = c.get("time", 0)
        if t < 0:
            continue
        bucket = (t // 300) * 300
        groups[bucket].append(c)

    result = []
    for bucket in sorted(groups.keys()):
        g = groups[bucket]
        result.append(
            {
                "time": bucket,
                "open": g[0]["open"],
                "high": max(c["high"] for c in g),
                "low": min(c["low"] for c in g),
                "close": g[-1]["close"],
            }
        )
    return result


async def get_candles_cached(symbol: str, count: int = 80) -> list[dict]:
    """
    Primary API for getting candles.

    Fast path (cache hit):   returns immediately, no browser, scales to any number of users.
    Slow path (cache miss):  falls through to a live browser fetch (first request only
                             or if refresher hasn't loaded this pair yet).
    """
    # 1. Try cache first
    cached = get_cached(symbol)
    if cached:
        return cached[:count] if len(cached) >= count else cached

    # 2. Cache miss — live fetch (also populates cache for next time)
    logger.info("Cache miss for %s — falling back to live fetch", symbol)
    from services.pocket_browser import _get_candles_impl, _get_lock

    try:
        async with asyncio.timeout(90):
            async with _get_lock():
                candles = await _get_candles_impl(symbol, count)
        if candles:
            store(symbol, candles)
        return candles
    except TimeoutError:
        logger.error("Live fetch timed out for %s", symbol)
        return []
    except Exception as e:
        logger.error("Live fetch failed for %s: %s", symbol, e)
        return []


"""
Strategy Engine — entry point for signal calculation.
Validates candles, resamples to 1-min and 5-min, then runs the full decision engine.
"""
import logging
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services.analysis.candle_validator import validate_and_fix
from services.analysis.decision_engine import run_decision_engine, EngineResult
from services.candle_cache import resample_to_1m, resample_to_5m
from services.strategy_adaptation import update_strategy_statuses
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SignalResult:
    direction: str  # "BUY" | "SELL" | "NO_SIGNAL"
    confidence: int  # 0-5 stars for UI display
    details: dict  # full analysis breakdown


async def calculate_signal(
    candles: list[dict],
    raised_threshold: bool = False,
) -> SignalResult:
    """
    Receives a list of OHLC dicts:
        [{"open": float, "high": float, "low": float, "close": float, "time": int}, ...]
    Validates, cleans, resamples to 5-min, then runs the decision engine.

    Args:
        candles: list of 1-min OHLC candles from cache
        raised_threshold: True after 2 consecutive losses → raise min confidence to 70
    """
    n_raw = len(candles)

    # Strategy adaptation: refresh statuses (cached 60s — no-op most of the time)
    try:
        await update_strategy_statuses()
    except Exception:
        pass  # never block signal calculation

    # ── Validate & clean ──────────────────────────────────────────────────────
    df, val = validate_and_fix(candles)

    if not val.ok or df is None or len(df) < 15:
        logger.warning(
            "Not enough usable candles: raw=%d, clean=%d, issues=%s",
            n_raw,
            val.candles_after_clean,
            val.issues,
        )
        return SignalResult(
            "NO_SIGNAL",
            0,
            {
                "direction": "NO_SIGNAL",
                "error": "not_enough_candles",
                "candles_raw": n_raw,
                "candles_clean": val.candles_after_clean,
                "reject_reason": "validation_failed",
                "debug": {
                    "candles_count": n_raw,
                    "candles_after_clean": val.candles_after_clean,
                    "order": val.order,
                    "issues": val.issues,
                    "reject_reason": "validation_failed",
                },
            },
        )

    logger.info(
        "Candles OK: %d raw → %d clean | order=%s | last=%.6f | avg_body=%.4f%%",
        n_raw,
        len(df),
        val.order,
        val.last_close,
        val.avg_body_pct,
    )

    # ── Resample 15s → 1-min (intermediate context) ───────────────────────────
    df1m_ctx = None
    candles_1m: list[dict] = []  # kept in scope so 5m block can reuse it
    try:
        import pandas as pd

        times_ok = sum(1 for c in candles if c.get("time", 0) > 0)
        logger.info(
            "1m resample input: %d candles, %d with valid time (first=%s last=%s)",
            len(candles),
            times_ok,
            candles[0].get("time") if candles else "N/A",
            candles[-1].get("time") if candles else "N/A",
        )
        candles_1m = resample_to_1m(candles)
        logger.info(
            "1-min resample: %d raw 15s → %d 1m bars", len(candles), len(candles_1m)
        )
        if len(candles_1m) >= 5:
            df1m_ctx = pd.DataFrame(candles_1m)
            for col in ("open", "high", "low", "close"):
                df1m_ctx[col] = df1m_ctx[col].astype(float)
        else:
            logger.warning(
                "1-min candles too few (%d) — ctx_1m disabled",
                len(candles_1m),
            )
    except Exception as e:
        logger.warning("1-min resampling failed: %s", e, exc_info=True)

    # ── Resample 1m → 5-min (macro context) ───────────────────────────────────
    # NOTE: resample_to_5m expects 1-min input, NOT raw 15s candles.
    # Using candles_1m (already resampled, with proper synthetic timestamps if needed)
    # ensures correct 5m grouping even when original candles have time=0.
    df5m = None
    try:
        import pandas as pd

        candles_5m = resample_to_5m(candles_1m)
        logger.info(
            "5-min resample: %d 1m bars → %d 5m bars", len(candles_1m), len(candles_5m)
        )
        if len(candles_5m) >= 4:
            df5m = pd.DataFrame(candles_5m)
            for col in ("open", "high", "low", "close"):
                df5m[col] = df5m[col].astype(float)
        else:
            logger.warning(
                "5-min candles too few (%d) — df5m disabled", len(candles_5m)
            )
    except Exception as e:
        logger.warning("5-min resampling failed: %s", e, exc_info=True)

    # ── Decision Engine ───────────────────────────────────────────────────────
    eng: EngineResult = run_decision_engine(
        df1m=df,
        df5m=df5m,
        df1m_ctx=df1m_ctx,
        raised_threshold=raised_threshold,
        n_bars_15s=len(df),
        n_bars_1m=len(df1m_ctx) if df1m_ctx is not None else 0,
        n_bars_5m=len(df5m) if df5m is not None else 0,
    )

    logger.info(
        "Signal result: %s | strategy=%s | conf_raw=%.0f | mode=%s | reason=%s",
        eng.direction,
        eng.strategy_name or "—",
        eng.confidence_raw,
        eng.market_mode,
        eng.reasoning if eng.direction == "NO_SIGNAL" else "ok",
    )

    # ── Detailed condition log (one line per strategy) ────────────────────────
    _log_conditions(eng)

    # ── Build details dict for signal_service ─────────────────────────────────
    details = {
        "direction": eng.direction,
        "confidence_raw": eng.confidence_raw,
        "confidence_5": eng.stars,
        "signal_quality": eng.quality,
        "primary_strategy": eng.strategy_name,
        "market_mode": eng.market_mode,
        "market_mode_strength": eng.market_mode_strength,
        "expiry_hint": eng.expiry_hint,
        "reasoning": eng.reasoning,
        "conditions_met": eng.conditions_met,
        "total_conditions": eng.total_conditions,
        # Legacy field aliases (for signal_service / format_signal_message compat)
        "regime": _mode_to_regime(eng.market_mode),
        "reject_reason": "" if eng.direction != "NO_SIGNAL" else eng.reasoning,
        "hard_conflicts": [] if eng.direction != "NO_SIGNAL" else [eng.reasoning],
        "debug": {
            **eng.debug,
            "candles_raw": n_raw,
            "candles_clean": len(df),
            "order": val.order,
            "avg_body_pct": round(val.avg_body_pct, 5),
            # last_close is used by outcome_tracker to determine WIN/LOSS
            "last_close": val.last_close,
        },
    }

    return SignalResult(
        direction=eng.direction,
        confidence=eng.stars,
        details=details,
    )


def _log_conditions(eng: "EngineResult") -> None:
    """Log per-strategy condition breakdown as searchable one-liners."""
    strategies: dict = eng.debug.get("strategies", {})
    if not strategies:
        return

    ctx_note = eng.debug.get("ctx_macro_note", "")
    ctx_up_1m = eng.debug.get("ctx_up_1m", False)
    ctx_dn_1m = eng.debug.get("ctx_dn_1m", False)
    ctx_macro_up = eng.debug.get("ctx_macro_up", False)
    ctx_macro_dn = eng.debug.get("ctx_macro_dn", False)
    conf_before = eng.debug.get("conf_before_multipliers", 0)
    conf_after = eng.debug.get("conf_after_multipliers", eng.confidence_raw)
    threshold = eng.debug.get("min_threshold", 46)
    used_tier = eng.debug.get("used_tier", "?")
    ind = eng.debug.get("indicators", {})
    lvl = eng.debug.get("levels", {})

    logger.info(
        "  ┌─ MODE=%s(%s%%) tier=%s | ctx_1m=%s%s macro=%s%s [%s]",
        eng.market_mode,
        round(eng.market_mode_strength),
        used_tier,
        "↑" if ctx_up_1m else "·",
        "↓" if ctx_dn_1m else "·",
        "↑" if ctx_macro_up else "·",
        "↓" if ctx_macro_dn else "·",
        ctx_note,
    )
    logger.info(
        "  │  IND: EMA5=%.6f EMA13=%.6f RSI=%.1f Stoch=%.0f/%.0f ATR_ratio=%.3f BB_bw=%.5f",
        ind.get("ema5", 0),
        ind.get("ema13", 0),
        ind.get("rsi", 0),
        ind.get("stoch_k", 0),
        ind.get("stoch_d", 0),
        ind.get("atr_ratio", 0),
        ind.get("bb_bw", 0),
    )
    logger.info(
        "  │  LVL: sup=%.6f(%.3f%%) res=%.6f(%.3f%%) n_sup=%d n_res=%d",
        lvl.get("nearest_sup", 0),
        lvl.get("dist_sup_pct", 0),
        lvl.get("nearest_res", 0),
        lvl.get("dist_res_pct", 0),
        lvl.get("n_supports", 0),
        lvl.get("n_resistances", 0),
    )

    for sname, sd in strategies.items():
        if sd.get("skipped"):
            continue
        direction = sd.get("direction", "NONE")
        conf = sd.get("confidence", 0)
        met = sd.get("conditions_met", 0)
        total = sd.get("total", 0)
        pct = sd.get("pct", 0)
        tier = sd.get("tier", "?")
        early_rej = sd.get("early_reject")
        conds: dict = sd.get("conditions", {})

        if early_rej:
            logger.info("  │  [%s/%s] SKIPPED early_reject=%s", sname, tier, early_rej)
            continue

        cond_parts = []
        for k, v in conds.items():
            if isinstance(v, bool):
                cond_parts.append(f"{'✓' if v else '✗'}{k}")
            elif k == "pattern_type":
                cond_parts.append(f"pat={v}")

        marker = "►" if sname == eng.strategy_name else "│"
        logger.info(
            "  %s [%s/%s] %s conf=%.0f met=%d/%d(%d%%) | %s",
            marker,
            sname,
            tier,
            direction,
            conf,
            met,
            total,
            pct,
            "  ".join(cond_parts) or "—",
        )

    if eng.direction != "NO_SIGNAL":
        logger.info(
            "  └─ RESULT %s conf %.0f→%.0f (thr=%d) expiry=%s",
            eng.direction,
            conf_before,
            conf_after,
            threshold,
            eng.expiry_hint,
        )
    else:
        logger.info("  └─ NO_SIGNAL: %s", eng.reasoning)


def _mode_to_regime(mode: str) -> str:
    """Map new market mode names to old regime strings used by signal formatter."""
    return {
        "TRENDING_UP": "uptrend",
        "TRENDING_DOWN": "downtrend",
        "RANGE": "range",
        "VOLATILE": "chaotic_noise",
        "SQUEEZE": "range",
    }.get(mode, "range")


"""
Indicator Calculator — fast periods tuned for 1-minute binary options.

Periods:
  EMA(5), EMA(13), EMA(21)
  RSI(7)
  Stochastic(5, 3, 3)
  Momentum(5)
  ATR(10)
  Bollinger Bands(15, 2.0)
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class Indicators:
    # EMAs
    ema5: float
    ema13: float
    ema21: float
    ema5_series: pd.Series
    ema13_series: pd.Series
    ema21_series: pd.Series

    # RSI(7)
    rsi: float  # current RSI (last candle)
    rsi_prev: float  # RSI of second-to-last candle (real, not stub)

    # Stochastic(5,3,3)
    stoch_k: float  # smoothed %K
    stoch_d: float  # %D
    stoch_k_prev: float

    # Momentum(5)
    momentum: float  # price[now] - price[now-5]
    momentum_prev: float

    # ATR(10)
    atr: float
    atr_avg30: float  # average ATR over last 30 bars (historical baseline)
    atr_ratio: float  # atr / atr_avg30

    # Bollinger Bands(15, 2.0)
    bb_upper: float
    bb_lower: float
    bb_mid: float
    bb_bw: float  # bandwidth = (upper - lower) / mid
    bb_bw_prev: float  # bandwidth 5 bars ago (for squeeze detection)


def calculate_indicators(df: pd.DataFrame) -> Indicators:
    """Calculate all indicators from a 1-min OHLC DataFrame."""
    n = len(df)
    close = df["close"]
    high = df["high"]
    low = df["low"]

    # ── EMAs ─────────────────────────────────────────────────────────────────
    ema5_s = close.ewm(span=5, adjust=False).mean()
    ema13_s = close.ewm(span=13, adjust=False).mean()
    ema21_s = close.ewm(span=21, adjust=False).mean()

    # ── RSI(7) ────────────────────────────────────────────────────────────────
    rsi_val = _rsi(close, 7)
    rsi_prev_val = _rsi(close.iloc[:-1], 7) if n >= 9 else rsi_val

    # ── Stochastic(5, 3, 3) ───────────────────────────────────────────────────
    stoch_k_val, stoch_d_val, stoch_k_prev_val = _stochastic(
        high, low, close, k=5, smooth_k=3, d=3
    )

    # ── Momentum(5) ───────────────────────────────────────────────────────────
    if n > 5:
        mom_now = float(close.iloc[-1]) - float(close.iloc[-6])
        mom_prev = float(close.iloc[-2]) - float(close.iloc[-7]) if n > 6 else 0.0
    else:
        mom_now = mom_prev = 0.0

    # ── ATR(10) ───────────────────────────────────────────────────────────────
    tr = pd.concat(
        [
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr_val = float(tr.rolling(10).mean().iloc[-1]) if n >= 10 else float(tr.mean())
    hist_n = min(30, n)
    atr_avg = float(tr.rolling(10).mean().iloc[-hist_n:].mean()) if n >= 10 else atr_val
    atr_r = atr_val / atr_avg if atr_avg > 0 else 1.0

    # ── Bollinger Bands(15, 2.0) ──────────────────────────────────────────────
    bb_p = min(15, n)
    bb_mid_s = close.rolling(bb_p).mean()
    bb_std_s = close.rolling(bb_p).std()
    bb_u = float((bb_mid_s + 2.0 * bb_std_s).iloc[-1])
    bb_l = float((bb_mid_s - 2.0 * bb_std_s).iloc[-1])
    bb_m = float(bb_mid_s.iloc[-1])
    bb_bw_now = (bb_u - bb_l) / bb_m if bb_m else 0.0

    if n >= bb_p + 5:
        bb_bw_p = float(
            (bb_mid_s + 2.0 * bb_std_s).iloc[-6] - (bb_mid_s - 2.0 * bb_std_s).iloc[-6]
        )
        bb_bw_p = (
            bb_bw_p / float(bb_mid_s.iloc[-6])
            if float(bb_mid_s.iloc[-6])
            else bb_bw_now
        )
    else:
        bb_bw_p = bb_bw_now

    return Indicators(
        ema5=float(ema5_s.iloc[-1]),
        ema13=float(ema13_s.iloc[-1]),
        ema21=float(ema21_s.iloc[-1]),
        ema5_series=ema5_s,
        ema13_series=ema13_s,
        ema21_series=ema21_s,
        rsi=rsi_val,
        rsi_prev=rsi_prev_val,
        stoch_k=stoch_k_val,
        stoch_d=stoch_d_val,
        stoch_k_prev=stoch_k_prev_val,
        momentum=mom_now,
        momentum_prev=mom_prev,
        atr=atr_val,
        atr_avg30=atr_avg,
        atr_ratio=atr_r,
        bb_upper=bb_u,
        bb_lower=bb_l,
        bb_mid=bb_m,
        bb_bw=bb_bw_now,
        bb_bw_prev=bb_bw_p,
    )


# ── Internal helpers ──────────────────────────────────────────────────────────


def _rsi(close: pd.Series, period: int = 7) -> float:
    try:
        import pandas_ta as ta

        s = ta.rsi(close, length=period)
        v = float(s.iloc[-1]) if s is not None else 50.0
        return v if not np.isnan(v) else 50.0
    except Exception:
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        ag = gain.ewm(com=period - 1, min_periods=period).mean()
        al = loss.ewm(com=period - 1, min_periods=period).mean()
        rs = ag / al.replace(0, np.nan)
        r = 100 - (100 / (1 + rs))
        v = float(r.iloc[-1])
        return v if not np.isnan(v) else 50.0


def _stochastic(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    k: int = 5,
    smooth_k: int = 3,
    d: int = 3,
) -> tuple[float, float, float]:
    """Returns (smooth_k_now, d_now, smooth_k_prev)."""
    try:
        import pandas_ta as ta

        s = ta.stoch(high, low, close, k=k, d=d, smooth_k=smooth_k)
        col_k = f"STOCHk_{k}_{d}_{smooth_k}"
        col_d = f"STOCHd_{k}_{d}_{smooth_k}"
        if s is not None and col_k in s.columns:
            kv = float(s[col_k].iloc[-1])
            dv = float(s[col_d].iloc[-1]) if col_d in s.columns else kv
            kp = float(s[col_k].iloc[-2]) if len(s) >= 2 else kv
            return (
                kv if not np.isnan(kv) else 50.0,
                dv if not np.isnan(dv) else 50.0,
                kp if not np.isnan(kp) else 50.0,
            )
    except Exception:
        pass
    # Manual fallback
    ll = low.rolling(k).min()
    hh = high.rolling(k).max()
    denom = (hh - ll).replace(0, np.nan)
    raw_k = 100 * (close - ll) / denom
    sk = raw_k.rolling(smooth_k).mean()
    sd = sk.rolling(d).mean()
    kv = float(sk.iloc[-1]) if not np.isnan(float(sk.iloc[-1])) else 50.0
    dv = float(sd.iloc[-1]) if not np.isnan(float(sd.iloc[-1])) else kv
    kp = float(sk.iloc[-2]) if len(sk) >= 2 else kv
    return kv, dv, kp


"""
Market Mode Detection — Layer 2
Classifies current market into one of 5 modes that determine which strategies to run.

Modes:
  TRENDING_UP   — clear uptrend on 1-min and 5-min
  TRENDING_DOWN — clear downtrend on 1-min and 5-min
  RANGE         — oscillating between identifiable S/R
  VOLATILE      — ATR spike, large candles, no clear direction
  SQUEEZE       — compressed ATR, Bollinger Bands narrowing — pending breakout
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class MarketMode:
    mode: str  # TRENDING_UP | TRENDING_DOWN | RANGE | VOLATILE | SQUEEZE
    strength: float  # 0-100 — how clearly the mode is established
    trend_up: bool  # True if any upward bias (used for 5-min context)
    trend_down: bool
    explanation: str
    debug: dict


def detect_market_mode(
    df1m: pd.DataFrame, df5m: pd.DataFrame | None = None
) -> MarketMode:
    """
    Determine market mode from 1-min candles (primary) and 5-min candles (context).
    Requires at least 25 1-min candles.
    """
    n = len(df1m)
    if n < 15:
        return MarketMode(
            "RANGE",
            40.0,
            False,
            False,
            "Мало данных — считаем боковым рынком",
            {"n": n},
        )

    close = df1m["close"]
    high = df1m["high"]
    low = df1m["low"]
    open_ = df1m["open"]

    # ── EMAs (1-min) ─────────────────────────────────────────────────────────
    ema5 = close.ewm(span=5, adjust=False).mean()
    ema13 = close.ewm(span=13, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()

    ema5_now = float(ema5.iloc[-1])
    ema13_now = float(ema13.iloc[-1])
    ema21_now = float(ema21.iloc[-1])

    # EMA alignment check over last 5 candles
    lookback = min(5, n)
    ema5_arr = ema5.iloc[-lookback:].values
    ema13_arr = ema13.iloc[-lookback:].values
    ema21_arr = ema21.iloc[-lookback:].values

    ema_bull_bars = int(np.sum((ema5_arr > ema13_arr) & (ema13_arr > ema21_arr)))
    ema_bear_bars = int(np.sum((ema5_arr < ema13_arr) & (ema13_arr < ema21_arr)))

    ema5_vs_21_spread = (
        abs(ema5_now - ema21_now) / float(close.iloc[-1]) * 100
        if float(close.iloc[-1])
        else 0.0
    )
    ema_aligned_up = ema_bull_bars >= 4
    ema_aligned_down = ema_bear_bars >= 4
    ema_flat = ema5_vs_21_spread < 0.03

    # ── ATR ───────────────────────────────────────────────────────────────────
    tr = pd.concat(
        [
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr10 = float(tr.rolling(10).mean().iloc[-1]) if n >= 10 else float(tr.mean())
    hist_bars = min(30, n)
    atr_hist = (
        float(tr.rolling(10).mean().iloc[-hist_bars:].mean()) if n >= 10 else atr10
    )
    atr_ratio = atr10 / atr_hist if atr_hist > 0 else 1.0

    # ── Bollinger Bands (15, 2.0) ─────────────────────────────────────────────
    bb_period = min(15, n)
    bb_mid = close.rolling(bb_period).mean()
    bb_std = close.rolling(bb_period).std()
    bb_bw_now = float((bb_std * 4 / bb_mid).iloc[-1]) if float(bb_mid.iloc[-1]) else 0.0

    # Bandwidth trend: is it narrowing?
    if n >= bb_period + 5:
        bb_bw_prev = float((bb_std * 4 / bb_mid).iloc[-6])
        bb_narrowing = bb_bw_now < bb_bw_prev * 0.9
    else:
        bb_narrowing = False

    # ── Candle direction stats ────────────────────────────────────────────────
    recent_n = min(7, n)
    bull_pct = float((close.iloc[-recent_n:] > open_.iloc[-recent_n:]).mean() * 100)
    body_abs = (close - open_).abs()
    avg_body = float(body_abs.rolling(min(10, n)).mean().iloc[-1])
    last5_body = float(body_abs.iloc[-min(5, n) :].mean())
    body_ratio = last5_body / avg_body if avg_body > 0 else 1.0

    # ── Swing structure (last 20 bars) ────────────────────────────────────────
    lb = min(20, n)
    seg = max(3, lb // 3)
    h = high.iloc[-lb:]
    l = low.iloc[-lb:]
    h1 = float(h.iloc[:seg].max())
    h2 = float(h.iloc[seg : 2 * seg].max())
    h3 = float(h.iloc[2 * seg :].max())
    l1 = float(l.iloc[:seg].min())
    l2 = float(l.iloc[seg : 2 * seg].min())
    l3 = float(l.iloc[2 * seg :].min())
    hh = h3 > h2 > h1
    hl = l3 > l2 > l1
    lh = h3 < h2 < h1
    ll = l3 < l2 < l1

    # ── 5-min context ─────────────────────────────────────────────────────────
    ctx_up = False
    ctx_down = False
    if df5m is not None and len(df5m) >= 5:
        c5 = df5m["close"]
        e21 = float(c5.ewm(span=min(21, len(df5m)), adjust=False).mean().iloc[-1])
        e50 = float(c5.ewm(span=min(50, len(df5m)), adjust=False).mean().iloc[-1])
        ctx_up = e21 > e50
        ctx_down = e21 < e50

    # ── VOLATILE: ATR spike + large candles + no clear direction ──────────────
    is_volatile = (
        atr_ratio > 1.5
        and body_ratio > 1.3
        and not ema_aligned_up
        and not ema_aligned_down
    )
    if is_volatile:
        return MarketMode(
            "VOLATILE",
            min(100, atr_ratio * 40),
            ctx_up,
            ctx_down,
            f"Высокая волатильность: ATR×{atr_ratio:.2f}, тела ×{body_ratio:.2f}",
            {"atr_ratio": round(atr_ratio, 2), "body_ratio": round(body_ratio, 2)},
        )

    # ── SQUEEZE: compressed ATR + narrowing BB + small bodies ─────────────────
    is_squeeze = atr_ratio < 0.6 and bb_narrowing and body_ratio < 0.5
    if is_squeeze:
        return MarketMode(
            "SQUEEZE",
            min(100, (1 - atr_ratio) * 80),
            ctx_up,
            ctx_down,
            f"Сжатие: ATR×{atr_ratio:.2f}, BB сужаются, тела ×{body_ratio:.2f}",
            {"atr_ratio": round(atr_ratio, 2), "bb_narrowing": bb_narrowing},
        )

    # ── TRENDING_UP ───────────────────────────────────────────────────────────
    up_pts = 0
    up_pts += 40 if ema_aligned_up else 0
    up_pts += 25 if (hh and hl) else (12 if (hh or hl) else 0)
    up_pts += 20 if bull_pct > 65 else (10 if bull_pct > 55 else 0)
    up_pts += 10 if ctx_up else 0

    # ── TRENDING_DOWN ─────────────────────────────────────────────────────────
    dn_pts = 0
    dn_pts += 40 if ema_aligned_down else 0
    dn_pts += 25 if (lh and ll) else (12 if (lh or ll) else 0)
    dn_pts += 20 if bull_pct < 35 else (10 if bull_pct < 45 else 0)
    dn_pts += 10 if ctx_down else 0

    if up_pts >= 55 and up_pts > dn_pts + 15:
        return MarketMode(
            "TRENDING_UP",
            min(100.0, float(up_pts)),
            True,
            False,
            f"Восходящий тренд: EMA выровнены={ema_aligned_up}, свинги HH={hh}/HL={hl}, бычьих {bull_pct:.0f}%",
            {
                "up_pts": up_pts,
                "ema_aligned": ema_aligned_up,
                "bull_pct": round(bull_pct),
            },
        )

    if dn_pts >= 55 and dn_pts > up_pts + 15:
        return MarketMode(
            "TRENDING_DOWN",
            min(100.0, float(dn_pts)),
            False,
            True,
            f"Нисходящий тренд: EMA выровнены={ema_aligned_down}, свинги LH={lh}/LL={ll}, медвежьих {100 - bull_pct:.0f}%",
            {
                "dn_pts": dn_pts,
                "ema_aligned": ema_aligned_down,
                "bear_pct": round(100 - bull_pct),
            },
        )

    # ── RANGE ─────────────────────────────────────────────────────────────────
    range_strength = max(0, 80 - abs(up_pts - dn_pts) * 2) if ema_flat else 60
    return MarketMode(
        "RANGE",
        float(range_strength),
        ctx_up,
        ctx_down,
        f"Боковой рынок: EMA flat={ema_flat}, ATR×{atr_ratio:.2f}",
        {"up_pts": up_pts, "dn_pts": dn_pts, "ema_flat": ema_flat},
    )


"""
Support / Resistance Level Detection
- Detects swing highs/lows with window of 3 bars
- Clusters levels within 0.15% tolerance
- Requires 2+ touches to consider a level significant
- Also merges levels found on 5-min data (stronger levels)
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass, field


@dataclass
class LevelSet:
    supports: list[float]
    resistances: list[float]
    strong_sup: list[float]  # 3+ touches (high-confidence)
    strong_res: list[float]
    nearest_sup: float
    nearest_res: float
    dist_to_sup_pct: float
    dist_to_res_pct: float


def detect_levels(df1m: pd.DataFrame, df5m: pd.DataFrame | None = None) -> LevelSet:
    price = float(df1m["close"].iloc[-1])
    if price == 0:
        price = 1.0

    # 1-min swing levels
    raw_res_1m = _swing_highs(df1m["high"], window=3)
    raw_sup_1m = _swing_lows(df1m["low"], window=3)

    # 5-min swing levels (weighted more — but kept separate so we can merge)
    raw_res_5m: list[float] = []
    raw_sup_5m: list[float] = []
    if df5m is not None and len(df5m) >= 7:
        raw_res_5m = _swing_highs(df5m["high"], window=2)
        raw_sup_5m = _swing_lows(df5m["low"], window=2)

    # Merge and cluster
    all_res = raw_res_1m + raw_res_5m
    all_sup = raw_sup_1m + raw_sup_5m

    res_clusters = _cluster_with_touches(all_res, tol_pct=0.0015)
    sup_clusters = _cluster_with_touches(all_sup, tol_pct=0.0015)

    # Filter relevant levels
    resistances = sorted(
        [l for l, t in res_clusters if l > price * 0.9998], key=lambda x: x
    )
    supports = sorted(
        [l for l, t in sup_clusters if l < price * 1.0002],
        key=lambda x: x,
        reverse=True,
    )

    strong_res = sorted(
        [l for l, t in res_clusters if l > price * 0.9998 and t >= 2], key=lambda x: x
    )
    strong_sup = sorted(
        [l for l, t in sup_clusters if l < price * 1.0002 and t >= 2],
        key=lambda x: x,
        reverse=True,
    )

    # Fallback if nothing found
    if not resistances:
        resistances = [float(df1m["high"].iloc[-20:].max())]
    if not supports:
        supports = [float(df1m["low"].iloc[-20:].min())]

    nearest_res = resistances[0] if resistances else price * 1.005
    nearest_sup = supports[0] if supports else price * 0.995

    dist_res = max(0.0, (nearest_res - price) / price * 100)
    dist_sup = max(0.0, (price - nearest_sup) / price * 100)

    return LevelSet(
        supports=supports,
        resistances=resistances,
        strong_sup=strong_sup,
        strong_res=strong_res,
        nearest_sup=nearest_sup,
        nearest_res=nearest_res,
        dist_to_sup_pct=round(dist_sup, 4),
        dist_to_res_pct=round(dist_res, 4),
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _swing_highs(series: pd.Series, window: int = 3) -> list[float]:
    result = []
    arr = series.values
    for i in range(window, len(arr) - window):
        if arr[i] == arr[max(0, i - window) : i + window + 1].max():
            result.append(float(arr[i]))
    return result


def _swing_lows(series: pd.Series, window: int = 3) -> list[float]:
    result = []
    arr = series.values
    for i in range(window, len(arr) - window):
        if arr[i] == arr[max(0, i - window) : i + window + 1].min():
            result.append(float(arr[i]))
    return result


def _cluster_with_touches(
    levels: list[float], tol_pct: float = 0.0015
) -> list[tuple[float, int]]:
    """
    Cluster nearby levels and count touches (occurrences in same cluster).
    Returns list of (level_price, touch_count).
    """
    if not levels:
        return []
    levels = sorted(levels)
    clusters: list[tuple[float, int]] = []
    group = [levels[0]]

    for v in levels[1:]:
        if group[0] > 0 and (v - group[-1]) / group[0] <= tol_pct:
            group.append(v)
        else:
            clusters.append((float(np.mean(group)), len(group)))
            group = [v]
    clusters.append((float(np.mean(group)), len(group)))
    return clusters


"""
Decision Engine — Layer 4
Selects strategies based on market mode, picks the best signal,
applies multipliers, enforces thresholds, and returns final decision.

Architecture:
  Mode → select primary + secondary strategies
  → run all selected strategies
  → pick best (conditions_met >= 60%)
  → apply confirmation (±10 / ×0.75)
  → apply multipliers (floor ×0.60)
  → compare against thresholds: strong≥70, moderate≥52
  → map to 5-star system: ≥88→5, ≥80→4, ≥70→3, ≥52→2

Rate limiting (state object injected from outside):
  - min 90s between signals
  - max 3 consecutive same direction
  - after 2 losses: threshold raised to 70 for 5 min (caller enforces)
"""
from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass

import asyncio

from .market_mode import MarketMode, detect_market_mode
from .indicators import Indicators, calculate_indicators
from .levels import LevelSet, detect_levels
from .strategies import (
    ema_bounce_strategy,
    squeeze_breakout_strategy,
    level_bounce_strategy,
    level_breakout_strategy,
    rsi_reversal_strategy,
    micro_breakout_strategy,
    divergence_strategy,
)

# Strategy adaptation — optional import, falls back gracefully if unavailable
try:
    from services.strategy_adaptation import is_strategy_enabled
except ImportError:

    def is_strategy_enabled(name: str) -> bool:
        return True  # type: ignore[misc]


logger = logging.getLogger(__name__)


# ── Mode → strategy routing ───────────────────────────────────────────────────
# PRIMARY strategies run first.  Secondary strategies run ONLY if no primary fires.
# RSI Reversal: secondary in RANGE and SQUEEZE only (removed from TRENDING/VOLATILE).
# Micro Breakout: VOLATILE primary only (requires ATR ≥ 0.55).
# Secondary threshold: 58 (vs primary 52) — weaker signals filtered harder.
_MODE_STRATEGIES: dict[str, dict] = {
    # RSI Reversal removed from TRENDING and VOLATILE — counter-trend reversals
    # in a confirmed trend direction have poor reliability at 1-2 min expiry.
    # level_breakout added: breakouts above/below 1m levels work well in trending markets.
    "TRENDING_UP": {
        "primary": ["ema_bounce", "squeeze_breakout", "level_breakout", "level_bounce"],
        "secondary": ["divergence"],
    },
    "TRENDING_DOWN": {
        "primary": ["ema_bounce", "squeeze_breakout", "level_breakout", "level_bounce"],
        "secondary": ["divergence"],
    },
    "RANGE": {
        # Level Bounce + EMA Bounce are the main 1-2 min strategies in ranging markets
        # RSI Reversal allowed here only — range is its natural habitat
        # level_breakout in secondary — can fire on range breakouts
        "primary": ["level_bounce", "ema_bounce"],
        "secondary": [
            "level_breakout",
            "divergence",
            "rsi_reversal",
            "squeeze_breakout",
            "micro_breakout",
        ],
    },
    "VOLATILE": {
        # Micro Breakout only allowed here where ATR is high enough
        # level_breakout primary here — volatile markets have real breakouts
        "primary": [
            "micro_breakout",
            "squeeze_breakout",
            "level_breakout",
            "level_bounce",
        ],
        "secondary": ["divergence"],
    },
    "SQUEEZE": {
        "primary": ["squeeze_breakout", "ema_bounce", "level_bounce"],
        "secondary": ["level_breakout", "divergence", "rsi_reversal"],
    },
}

_STRATEGY_FNS = {
    "ema_bounce": ema_bounce_strategy,
    "squeeze_breakout": squeeze_breakout_strategy,
    "level_bounce": level_bounce_strategy,
    "level_breakout": level_breakout_strategy,
    "rsi_reversal": rsi_reversal_strategy,
    "micro_breakout": micro_breakout_strategy,
    "divergence": divergence_strategy,
}


@dataclass
class EngineResult:
    direction: str  # "BUY" | "SELL" | "NO_SIGNAL"
    confidence_raw: float  # 0-100 before star mapping
    stars: int  # 1-5
    quality: str  # "strong" | "moderate" | "weak" | "none"
    strategy_name: str
    market_mode: str
    market_mode_strength: float
    reasoning: str
    conditions_met: int
    total_conditions: int
    expiry_hint: str  # "1m" | "2m" (suggested trade expiry)
    debug: dict


def run_decision_engine(
    df1m: pd.DataFrame,
    df5m: pd.DataFrame | None = None,
    df1m_ctx: pd.DataFrame
    | None = None,  # 1-min resampled from 15s — middle-tier context
    raised_threshold: bool = False,  # True = after 2 losses, min conf=70
    n_bars_15s: int = 0,
    n_bars_1m: int = 0,
    n_bars_5m: int = 0,
) -> EngineResult:
    """
    Full 4-layer analysis pipeline optimised for 1-2 min OTC expiry.

    Args:
        df1m:        15-sec OHLC DataFrame (entry timing), oldest→newest, min 20 rows
        df5m:        5-min resampled OHLC (macro context, optional)
        df1m_ctx:    1-min resampled OHLC (intermediate context, optional)
        raised_threshold: if True, minimum confidence is raised to 70
        n_bars_15s:  number of raw 15-sec bars (informational, for debug)
        n_bars_1m:   number of 1-min bars after resampling
        n_bars_5m:   number of 5-min bars after resampling
    """
    n = len(df1m)
    _bar_debug = {
        "n_bars_15s": n_bars_15s or n,
        "n_bars_1m": n_bars_1m,
        "n_bars_5m": n_bars_5m,
    }

    if n < 15:
        return _no_signal("Недостаточно данных (нужно ≥15 свечей)", {**_bar_debug})

    # ── Layer 1: Indicators ───────────────────────────────────────────────────
    try:
        ind = calculate_indicators(df1m)
    except Exception as e:
        logger.exception("Indicator calculation failed")
        return _no_signal(f"Ошибка расчёта индикаторов: {e}", {**_bar_debug})

    # snapshot of indicator values for NO_SIGNAL debug (computed once, reused)
    _ind_dbg: dict = {
        "indicators": {
            "atr": round(ind.atr, 6),
            "atr_ratio": round(ind.atr_ratio, 3),
            "rsi": round(ind.rsi, 1),
            "stoch_k": round(ind.stoch_k, 1),
            "stoch_d": round(ind.stoch_d, 1),
            "ema5": round(ind.ema5, 6),
            "ema13": round(ind.ema13, 6),
            "ema21": round(ind.ema21, 6),
            "bb_bw": round(ind.bb_bw, 5),
            "momentum": round(ind.momentum, 6),
        }
    }

    # ── Layer 2: Market Mode ──────────────────────────────────────────────────
    try:
        mode_obj = detect_market_mode(df1m, df5m)
    except Exception as e:
        logger.exception("Market mode detection failed")
        mode_obj = MarketMode("RANGE", 50.0, False, False, "Ошибка — считаем RANGE", {})

    # ── Support / Resistance levels ───────────────────────────────────────────
    try:
        levels = detect_levels(df1m, df5m)
    except Exception as e:
        logger.exception("Level detection failed")
        levels = LevelSet([], [], [], [], 0.0, 0.0, 0.0, 0.0)

    _lvl_dbg: dict = {
        "levels": {
            "nearest_sup": round(levels.nearest_sup, 6),
            "nearest_res": round(levels.nearest_res, 6),
            "dist_sup_pct": levels.dist_to_sup_pct,
            "dist_res_pct": levels.dist_to_res_pct,
            "n_supports": len(levels.supports),
            "n_resistances": len(levels.resistances),
        }
    }

    # ── Layer 3: Strategies ───────────────────────────────────────────────────
    routing = _MODE_STRATEGIES.get(mode_obj.mode, _MODE_STRATEGIES["RANGE"])

    # ── Multi-timeframe context ───────────────────────────────────────────────
    #
    # Layer A — short-term (1-3 min): EMA(3) vs EMA(8) on resampled 1-min bars
    #   Tells us the direction of the last few minutes.
    #
    # Layer B — macro trend: linear regression slope over ALL available 1-min bars
    #   50 fifteen-second bars → ~13 one-minute bars → slope is reliable without
    #   needing 8+ five-minute bars.  5m EMA(5/21) is skipped because 4 bars
    #   make it essentially noise.
    #
    # Bonus (+7) and penalty (×0.82) only fire when BOTH layers agree.

    # Layer A — 1m EMA direction
    ctx_up_1m = ctx_dn_1m = False
    if df1m_ctx is not None and len(df1m_ctx) >= 5:
        e3 = float(df1m_ctx["close"].ewm(span=3, adjust=False).mean().iloc[-1])
        e8 = float(df1m_ctx["close"].ewm(span=8, adjust=False).mean().iloc[-1])
        ctx_up_1m = e3 > e8
        ctx_dn_1m = e3 < e8

    # Layer B — macro slope (linear regression on 1-min closes)
    ctx_macro_up = ctx_macro_dn = False
    ctx_macro_note = "slope_na"
    if df1m_ctx is not None and len(df1m_ctx) >= 6:
        closes = df1m_ctx["close"].values.astype(float)
        x = np.arange(len(closes))
        slope = float(np.polyfit(x, closes, 1)[0])
        norm_slope = slope / float(closes.mean())  # fraction per 1-min bar
        ctx_macro_up = norm_slope > 5e-5  # +0.005%/bar → upward
        ctx_macro_dn = norm_slope < -5e-5  # -0.005%/bar → downward
        ctx_macro_note = f"1m_slope={norm_slope * 1e4:.1f}bp/bar"
    elif df5m is not None and len(df5m) >= 8:
        # Fallback: 5m EMA only when we have enough bars to be meaningful
        e5 = float(df5m["close"].ewm(span=5, adjust=False).mean().iloc[-1])
        e21 = float(df5m["close"].ewm(span=21, adjust=False).mean().iloc[-1])
        ctx_macro_up = e5 > e21
        ctx_macro_dn = e5 < e21
        ctx_macro_note = f"5m_ema5vs21 (n={len(df5m)})"

    # ctx_up/ctx_down — weak context flag passed to strategies (informational)
    ctx_up = ctx_up_1m or ctx_macro_up or mode_obj.trend_up
    ctx_down = ctx_dn_1m or ctx_macro_dn or mode_obj.trend_down

    debug_strategies: dict = {}

    def _run_batch(names: list[str]) -> list:
        """Run a list of strategy names, return fired candidates."""
        fired = []
        for name in names:
            fn = _STRATEGY_FNS.get(name)
            if fn is None:
                continue
            if not is_strategy_enabled(name):
                debug_strategies[name] = {"status": "DISABLED", "skipped": True}
                continue
            try:
                kwargs = dict(
                    df=df1m,
                    ind=ind,
                    levels=levels,
                    ctx_trend_up=ctx_up,
                    ctx_trend_down=ctx_down,
                )
                if name in (
                    "level_bounce",
                    "level_breakout",
                    "divergence",
                    "ema_bounce",
                ):
                    kwargs["mode"] = mode_obj.mode
                if name in ("level_bounce", "level_breakout"):
                    kwargs["df1m_ctx"] = df1m_ctx
                res = fn(**kwargs)
            except Exception as e:
                logger.warning("Strategy %s failed: %s", name, e)
                continue

            pct = (
                res.conditions_met / res.total_conditions
                if res.total_conditions > 0
                else 0
            )
            # Pull per-condition breakdown from strategy debug
            # Direction that fired determines which side's conditions to show
            if res.direction == "BUY":
                conds = res.debug.get("buy_conditions", {})
            elif res.direction == "SELL":
                conds = res.debug.get("sell_conditions", {})
            else:
                # NO_SIGNAL: show whichever side had more conditions met
                buy_c = res.debug.get("buy_conditions", {})
                sell_c = res.debug.get("sell_conditions", {})
                buy_n = res.debug.get("buy_met", 0)
                sell_n = res.debug.get("sell_met", 0)
                conds = buy_c if buy_n >= sell_n else sell_c
            debug_strategies[name] = {
                "direction": res.direction,
                "confidence": round(res.confidence, 1),
                "conditions_met": res.conditions_met,
                "total": res.total_conditions,
                "pct": round(pct * 100),
                "tier": "primary" if name in routing["primary"] else "secondary",
                "early_reject": res.debug.get("early_reject"),
                "conditions": conds,
            }
            if res.direction in ("BUY", "SELL") and pct >= 0.37 and res.confidence > 10:
                fired.append(res)
        return fired

    # PRIMARY first; secondaries only if no primary fires
    candidates = _run_batch(routing["primary"])
    used_tier = "primary"
    if not candidates:
        candidates = _run_batch(routing["secondary"])
        used_tier = "secondary"

    # ── Record condition frequencies (fire-and-forget, never blocks signal) ──
    try:
        from db.database import record_condition_evals as _rec_cond

        _evals = [
            (
                sname,
                {
                    k: v
                    for k, v in sd.get("conditions", {}).items()
                    if isinstance(v, bool)
                },
            )
            for sname, sd in debug_strategies.items()
            if not sd.get("skipped")
        ]
        if _evals:
            asyncio.create_task(_rec_cond(_evals))
    except Exception:
        pass

    if not candidates:
        return _no_signal(
            f"Ни одна стратегия не выполнила условия (режим={mode_obj.mode})",
            {
                "mode": mode_obj.mode,
                "mode_strength": round(mode_obj.strength, 1),
                "mode_debug": mode_obj.debug,
                "mode_explanation": mode_obj.explanation,
                "strategies": debug_strategies,
                "used_tier": used_tier,
                "ctx_up_1m": ctx_up_1m,
                "ctx_dn_1m": ctx_dn_1m,
                "ctx_macro_up": ctx_macro_up,
                "ctx_macro_dn": ctx_macro_dn,
                "ctx_macro_note": ctx_macro_note,
                **_bar_debug,
                **_ind_dbg,
                **_lvl_dbg,
            },
        )

    # ── Pick best candidate ────────────────────────────────────────────────────
    best = max(candidates, key=lambda r: r.confidence)

    # If two strategies disagree on direction → pick by confidence, then by priority
    _STRATEGY_PRIORITY = {
        "level_bounce": 1,
        "level_breakout": 2,
        "ema_bounce": 3,
        "squeeze_breakout": 4,
    }

    buy_cands = [r for r in candidates if r.direction == "BUY"]
    sell_cands = [r for r in candidates if r.direction == "SELL"]
    if buy_cands and sell_cands:
        best_buy = max(buy_cands, key=lambda r: r.confidence)
        best_sell = max(sell_cands, key=lambda r: r.confidence)
        if abs(best_buy.confidence - best_sell.confidence) < 6:
            # Confidence too close — break tie by strategy priority
            pri_buy = _STRATEGY_PRIORITY.get(best_buy.strategy_name, 99)
            pri_sell = _STRATEGY_PRIORITY.get(best_sell.strategy_name, 99)
            if pri_buy == pri_sell:
                # Same strategy on both sides (shouldn't happen) — skip
                return _no_signal(
                    f"Противоречие стратегий — равный conf и приоритет ({best_buy.strategy_name})",
                    {
                        "mode": mode_obj.mode,
                        "mode_strength": round(mode_obj.strength, 1),
                        "mode_debug": mode_obj.debug,
                        "strategies": debug_strategies,
                        "buy_conf": round(best_buy.confidence, 1),
                        "sell_conf": round(best_sell.confidence, 1),
                        **_bar_debug,
                        **_ind_dbg,
                        **_lvl_dbg,
                    },
                )
            # Lower priority number = higher priority strategy wins
            best = best_buy if pri_buy < pri_sell else best_sell
        else:
            best = best_buy if best_buy.confidence > best_sell.confidence else best_sell

    direction = best.direction
    conf_raw = best.confidence

    # ── Layer 4: Multipliers ───────────────────────────────────────────────────

    # 4a. Multi-timeframe context confirmation / penalty
    # Bonus (+7):  1m EMA direction AND macro slope BOTH agree with signal
    # Penalty ×0.82: BOTH oppose signal (true counter-trend, not just noise)
    ctx_up_strong = ctx_up_1m and ctx_macro_up
    ctx_dn_strong = ctx_dn_1m and ctx_macro_dn

    if direction == "BUY":
        if ctx_up_strong and best.conditions_met >= 5:
            conf_raw += 3  # 1m EMA + slope confirm upward (only solid signals)
        elif ctx_dn_strong:
            conf_raw *= 0.82  # both layers oppose → counter-trend penalty
    else:  # SELL
        if ctx_dn_strong and best.conditions_met >= 5:
            conf_raw += 3
        elif ctx_up_strong:
            conf_raw *= 0.82

    # 4b. Market mode strength multiplier
    mode_str_m = mode_obj.strength / 100.0  # 0-1
    # Only apply if confidence is moderate (don't destroy already-great signals)
    if conf_raw < 75:
        conf_raw = conf_raw * (0.88 + 0.12 * mode_str_m)

    # 4c. Hard floor: if after all multipliers conf < 60% of raw → apply ×0.60
    floor_conf = best.confidence * 0.60
    if conf_raw < floor_conf:
        conf_raw = floor_conf

    conf_raw = min(100.0, conf_raw)

    # 4d. Trend guard: penalise signals strongly against confirmed trend
    # Strong trend = market mode is TRENDING + EMA stack is fully aligned
    strong_down = mode_obj.mode == "TRENDING_DOWN" and ind.ema5 < ind.ema13 < ind.ema21
    strong_up = mode_obj.mode == "TRENDING_UP" and ind.ema5 > ind.ema13 > ind.ema21

    if direction == "BUY" and strong_down:
        conf_raw *= 0.50  # BUY against downtrend: conf halved → likely below threshold
    if direction == "SELL" and strong_up:
        conf_raw *= 0.50  # SELL against uptrend: conf halved → likely below threshold

    # ── Threshold check ────────────────────────────────────────────────────────
    # Hard floor: 55 for all tiers — no signal below 55 ever becomes a trade.
    # After 2 consecutive losses: threshold raises to 70.
    if raised_threshold:
        min_threshold = 70
    else:
        min_threshold = 55  # hard floor: no signal below 55 ever becomes a trade

    if conf_raw < min_threshold:
        return _no_signal(
            f"Уверенность {conf_raw:.0f} < порог {min_threshold} (tier={used_tier})",
            {
                "mode": mode_obj.mode,
                "mode_strength": round(mode_obj.strength, 1),
                "mode_debug": mode_obj.debug,
                "conf_raw": round(conf_raw, 1),
                "strategy": best.strategy_name,
                "strategies": debug_strategies,
                "used_tier": used_tier,
                "min_threshold": min_threshold,
                "ctx_up_1m": ctx_up_1m,
                "ctx_dn_1m": ctx_dn_1m,
                "ctx_macro_note": ctx_macro_note,
                **_bar_debug,
                **_ind_dbg,
                **_lvl_dbg,
            },
        )

    # ── Stars ──────────────────────────────────────────────────────────────────
    if conf_raw >= 75:
        stars = 5
    elif conf_raw >= 65:
        stars = 4
    else:
        stars = 3

    if conf_raw >= 75:
        quality = "strong"
    elif conf_raw >= 65:
        quality = "good"
    else:
        quality = "moderate"

    # ── Expiry hint ────────────────────────────────────────────────────────────
    expiry = _pick_expiry(best.strategy_name, quality)

    return EngineResult(
        direction=direction,
        confidence_raw=round(conf_raw, 1),
        stars=stars,
        quality=quality,
        strategy_name=best.strategy_name,
        market_mode=mode_obj.mode,
        market_mode_strength=round(mode_obj.strength, 1),
        reasoning=best.reasoning,
        conditions_met=best.conditions_met,
        total_conditions=best.total_conditions,
        expiry_hint=expiry,
        debug={
            **_bar_debug,
            "mode": mode_obj.mode,
            "mode_strength": round(mode_obj.strength, 1),
            "mode_debug": mode_obj.debug,
            "ctx_up": ctx_up,
            "ctx_down": ctx_down,
            "ctx_up_1m": ctx_up_1m,
            "ctx_dn_1m": ctx_dn_1m,
            "ctx_macro_up": ctx_macro_up,
            "ctx_macro_dn": ctx_macro_dn,
            "ctx_macro_note": ctx_macro_note,
            "ctx_up_strong": ctx_up_strong,
            "ctx_dn_strong": ctx_dn_strong,
            "used_tier": used_tier,
            "min_threshold": min_threshold,
            "strategies": debug_strategies,
            "best_strategy": best.strategy_name,
            "conf_before_multipliers": round(best.confidence, 1),
            "conf_after_multipliers": round(conf_raw, 1),
            "raised_threshold": raised_threshold,
            "indicators": {
                "ema5": round(ind.ema5, 6),
                "ema13": round(ind.ema13, 6),
                "ema21": round(ind.ema21, 6),
                "rsi": round(ind.rsi, 1),
                "stoch_k": round(ind.stoch_k, 1),
                "stoch_d": round(ind.stoch_d, 1),
                "atr": round(ind.atr, 6),
                "atr_ratio": round(ind.atr_ratio, 3),
                "bb_bw": round(ind.bb_bw, 4),
            },
            "levels": {
                "nearest_sup": round(levels.nearest_sup, 6),
                "nearest_res": round(levels.nearest_res, 6),
                "dist_sup_pct": levels.dist_to_sup_pct,
                "dist_res_pct": levels.dist_to_res_pct,
                "n_supports": len(levels.supports),
                "n_resistances": len(levels.resistances),
                "n_strong_sup": len(levels.strong_sup),
                "n_strong_res": len(levels.strong_res),
            },
        },
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _pick_expiry(strategy: str, quality: str) -> str:
    """Bounce/breakout → 1m, trend-follow → 2m."""
    if strategy in (
        "level_bounce",
        "level_breakout",
        "rsi_reversal",
        "divergence",
        "ema_bounce",
    ):
        return "1m"
    if strategy in ("squeeze_breakout", "micro_breakout"):
        return "1m"
    return "2m"


def _no_signal(reason: str, debug: dict) -> EngineResult:
    return EngineResult(
        direction="NO_SIGNAL",
        confidence_raw=0.0,
        stars=0,
        quality="none",
        strategy_name="",
        market_mode=debug.get("mode", ""),
        market_mode_strength=0.0,
        reasoning=reason,
        conditions_met=0,
        total_conditions=0,
        expiry_hint="",
        debug=debug,
    )


"""
Strategy 1 — EMA Bounce in Micro-Trend
Scenario: Price in a trend, pulls back to EMA(13), bounces continuing the trend.
Best in: TRENDING_UP / TRENDING_DOWN modes.

8 conditions, minimum 5 required (tighter than before).
Touch zone tightened to ±0.02%. Pullback requires 3/4 red candles + one below EMA13.
Condition 8 added: bounce candle must close with conviction.
No 5-min trend bonus — short expiry doesn't need higher-TF confirmation.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet


@dataclass
class StrategyResult:
    direction: str  # "BUY" | "SELL" | "NONE"
    confidence: float  # 0-100
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL = 8
_MIN_MET = 4


def ema_bounce_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
) -> StrategyResult:
    """
    Returns BUY / SELL / NONE with confidence 0-100.
    Requires >= 4 of 8 conditions met.

    Regime filter:
      TRENDING_UP   → only BUY (pullback INTO uptrend = BUY opportunity)
      TRENDING_DOWN → only SELL (pullback INTO downtrend = SELL opportunity)
      All others    → both directions allowed
    """
    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    n = len(df)

    if n < 8:
        return _none("Мало данных", {"early_reject": "n<8"})

    # Hard reject: dead market (EMA bounces work in mild volatility, so lower bar than breakouts)
    if ind.atr_ratio < 0.35:
        return _none(
            "ATR мёртвый — рынок стоит",
            {
                "early_reject": f"atr_ratio={round(ind.atr_ratio, 3)}<0.35",
                "atr_ratio": round(ind.atr_ratio, 3),
            },
        )

    price = close[-1]
    avg_body = (
        float(np.mean(np.abs(close[-min(10, n) :] - open_[-min(10, n) :]))) or 1e-8
    )

    buy_score, buy_met, buy_parts, buy_conds = _check_buy(
        close, open_, high, low, n, price, avg_body, ind
    )
    sell_score, sell_met, sell_parts, sell_conds = _check_sell(
        close, open_, high, low, n, price, avg_body, ind
    )

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Условия не выполнены"

    # Tiebreaker: if scores are equal, context trend breaks the tie
    buy_wins = (buy_met > sell_met) or (
        buy_met == sell_met and ctx_trend_up and not ctx_trend_down
    )
    sell_wins = (sell_met > buy_met) or (
        sell_met == buy_met and ctx_trend_down and not ctx_trend_up
    )

    if buy_wins and buy_met >= _MIN_MET:
        direction = "BUY"
        conditions_met = buy_met
        # Anchored curve: 4→40, 5→50, 6→60, 7→70, 8→80
        base_conf = 40 + max(0, buy_met - 4) * 10
        reason = " | ".join(buy_parts)
        # Precision touch bonus (very close to EMA13 — within 0.01%)
        if abs(low[-1] - ind.ema13) / price < 0.0001:
            base_conf += 5
        # Small candle shadow (conviction in direction)
        if (
            abs(close[-1] - open_[-1]) > 0
            and (high[-1] - close[-1]) < abs(close[-1] - open_[-1]) * 0.2
        ):
            base_conf += 3
        # RSI in comfortable buy zone
        if 45 <= ind.rsi <= 60:
            base_conf += 3

    elif sell_wins and sell_met >= _MIN_MET:
        direction = "SELL"
        conditions_met = sell_met
        # Same anchored curve as buy side: 4→40, 5→50, 6→60, 7→70, 8→80
        base_conf = 40 + max(0, sell_met - 4) * 10
        reason = " | ".join(sell_parts)
        if abs(high[-1] - ind.ema13) / price < 0.0001:
            base_conf += 5
        if (
            abs(close[-1] - open_[-1]) > 0
            and (close[-1] - low[-1]) < abs(close[-1] - open_[-1]) * 0.2
        ):
            base_conf += 3
        if 40 <= ind.rsi <= 55:
            base_conf += 3

    # ── Trend momentum penalty (FIX 1) ───────────────────────────────────────
    # A weak bull/bear ratio contradicts the trend classification.
    # If fewer than half the recent candles are in trend direction → -15.
    if direction != "NONE":
        recent_n = min(20, n)
        bull_pct = float(np.sum(close[-recent_n:] > open_[-recent_n:])) / recent_n * 100
        bear_pct = 100.0 - bull_pct
        if direction == "BUY" and mode == "TRENDING_UP" and bull_pct < 50:
            base_conf -= 15
            reason += f" | ⚠ Слабый тренд (бычьих {bull_pct:.0f}%) -15"
        if direction == "SELL" and mode == "TRENDING_DOWN" and bear_pct < 50:
            base_conf -= 15
            reason += f" | ⚠ Слабый тренд (медвежьих {bear_pct:.0f}%) -15"

    # ── Level proximity penalty (FIX 2) ──────────────────────────────────────
    # Buying near resistance or selling near support is high risk.
    # If within 0.02% of the opposing level → -15.
    if direction == "BUY" and levels.resistances:
        above_res = [r for r in levels.resistances if r > price]
        if above_res:
            nearest_res = min(above_res)
            dist_pct = (nearest_res - price) / price
            if dist_pct < 0.0002:  # within 0.02%
                base_conf -= 15
                reason += f" | ⚠ BUY близко к сопротивлению ({dist_pct * 100:.3f}%) -15"
    if direction == "SELL" and levels.supports:
        below_sup = [s for s in levels.supports if s < price]
        if below_sup:
            nearest_sup = max(below_sup)
            dist_pct = (price - nearest_sup) / price
            if dist_pct < 0.0002:  # within 0.02%
                base_conf -= 15
                reason += f" | ⚠ SELL близко к поддержке ({dist_pct * 100:.3f}%) -15"

    # ── Exhaustion hard gate ──────────────────────────────────────────────────
    # If RSI/Stoch shows extreme exhaustion in the direction of the signal,
    # the move is already played out — block the signal immediately.
    # SELL when RSI<25 or StochK<10 → price is oversold, bounce likely.
    # BUY  when RSI>75 or StochK>90 → price is overbought, reversal likely.
    if direction == "SELL" and (ind.rsi < 25 or ind.stoch_k < 10):
        return _none(
            f"SELL заблокирован: перепроданность (RSI={ind.rsi:.1f}, Stoch K={ind.stoch_k:.1f})",
            {
                "exhaustion_block": "sell_oversold",
                "rsi": round(ind.rsi, 1),
                "stoch_k": round(ind.stoch_k, 1),
            },
        )
    if direction == "BUY" and (ind.rsi > 75 or ind.stoch_k > 90):
        return _none(
            f"BUY заблокирован: перекупленность (RSI={ind.rsi:.1f}, Stoch K={ind.stoch_k:.1f})",
            {
                "exhaustion_block": "buy_overbought",
                "rsi": round(ind.rsi, 1),
                "stoch_k": round(ind.stoch_k, 1),
            },
        )

    # ── Regime direction filter ───────────────────────────────────────────────
    # In a confirmed trend, ema_bounce should only trade WITH the trend.
    # A pullback in TRENDING_UP is a BUY opportunity, never SELL.
    # A pullback in TRENDING_DOWN is a SELL opportunity, never BUY.
    if direction == "SELL" and mode == "TRENDING_UP":
        return _none(
            f"SELL заблокирован: режим TRENDING_UP (откат = BUY возможность)",
            {
                "regime_block": "sell_in_uptrend",
                "sell_met": sell_met,
                "buy_met": buy_met,
                "sell_conditions": sell_conds,
                "buy_conditions": buy_conds,
            },
        )
    if direction == "BUY" and mode == "TRENDING_DOWN":
        return _none(
            f"BUY заблокирован: режим TRENDING_DOWN (откат = SELL возможность)",
            {
                "regime_block": "buy_in_downtrend",
                "sell_met": sell_met,
                "buy_met": buy_met,
                "sell_conditions": sell_conds,
                "buy_conditions": buy_conds,
            },
        )

    return StrategyResult(
        direction=direction,
        confidence=max(0.0, min(100.0, base_conf)),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="ema_bounce",
        reasoning=reason,
        debug={
            "buy_met": buy_met,
            "sell_met": sell_met,
            "buy_conditions": buy_conds,
            "sell_conditions": sell_conds,
        },
    )


def _check_buy(close, open_, high, low, n, price, avg_body, ind: Indicators):
    met = 0
    parts = []
    conds: dict[str, bool] = {}
    check = min(5, n)

    # 1. EMA aligned up — relaxed: EMA5 > EMA13 in 3+ of 5 bars, OR EMA5 trending up + 2+ bars
    #    Also requires minimum EMA5-EMA21 spread ≥ 0.005% — flat EMAs don't count as aligned
    ema5_arr = ind.ema5_series.iloc[-check:].values
    ema13_arr = ind.ema13_series.iloc[-check:].values
    ema21_arr = ind.ema21_series.iloc[-check:].values
    ema5_slope = float(ema5_arr[-1]) - float(ema5_arr[0])
    ema_spread_pct = (
        abs(float(ema5_arr[-1]) - float(ema21_arr[-1])) / price if price else 0
    )
    ema_has_spread = ema_spread_pct >= 0.00005  # 0.005% minimum spread
    c1 = ema_has_spread and (
        (int(np.sum(ema5_arr > ema13_arr)) >= 3)
        or (ema5_slope > 0 and int(np.sum(ema5_arr > ema13_arr)) >= 2)
    )
    conds["ema_aligned_up"] = c1
    if c1:
        met += 1
        parts.append("EMA выровнены вверх")

    # 2. Price low touched EMA(13) zone — ±0.07% (widened for OTC spread tolerance)
    ema13_zone = ind.ema13 * 1.0007
    c2 = any(float(low[-i]) <= ema13_zone for i in range(1, min(4, n)))
    conds["price_near_ema13"] = c2
    if c2:
        met += 1
        parts.append("Коснулась EMA13 (±0.07%)")

    # 3. Candle closed ABOVE EMA(13)
    c3 = close[-1] > ind.ema13
    conds["close_above_ema13"] = c3
    if c3:
        met += 1
        parts.append("Закрылась выше EMA13")

    # 4. Bounce candle is bullish with body > 50% avg_body
    c4 = close[-1] > open_[-1] and abs(close[-1] - open_[-1]) > avg_body * 0.5
    conds["bounce_candle_bullish"] = c4
    if c4:
        met += 1
        parts.append("Бычья свеча отскока")

    # 5. Real pullback: 3 of 4 previous candles bearish AND at least one closed below EMA13
    c5 = False
    if n >= 5:
        pb_window = range(2, min(6, n))
        pb_bearish = [
            close[-i] < open_[-i] or abs(close[-i] - open_[-i]) < avg_body * 0.5
            for i in pb_window
        ]
        pb_below_ema = any(float(close[-i]) < ind.ema13 for i in pb_window)
        c5 = sum(pb_bearish) >= 3 and pb_below_ema
    conds["real_pullback"] = c5
    if c5:
        met += 1
        parts.append("Откат к EMA13 (3/4 свечи медвежьи, одна под EMA)")

    # 6. RSI(7) between 40 and 70 (not overheated, not oversold)
    c6 = 40 <= ind.rsi <= 70
    conds["rsi_ok"] = c6
    if c6:
        met += 1
        parts.append(f"RSI {ind.rsi:.0f} в норме")

    # 7. Stochastic %K > %D (relaxed: no strict cross required)
    c7 = ind.stoch_k > ind.stoch_d
    conds["stoch_turning_up"] = c7
    if c7:
        met += 1
        parts.append(f"Stoch K>D ({ind.stoch_k:.0f}>{ind.stoch_d:.0f})")

    # 8. Bounce candle shows conviction: body > 0.5× avg AND closes in upper 50% of range
    total_range = high[-1] - low[-1]
    c8 = (
        abs(close[-1] - open_[-1]) > avg_body * 0.5
        and total_range > 0
        and (close[-1] - low[-1]) / total_range > 0.5
    )
    conds["candle_conviction"] = c8
    if c8:
        met += 1
        parts.append("Свеча закрылась в верхней зоне диапазона")

    return met, met, parts, conds


def _check_sell(close, open_, high, low, n, price, avg_body, ind: Indicators):
    met = 0
    parts = []
    conds: dict[str, bool] = {}
    check = min(5, n)

    # 1. EMA aligned down — relaxed: EMA5 < EMA13 in 3+ of 5 bars, OR EMA5 trending down + 2+ bars
    #    Also requires minimum EMA5-EMA21 spread ≥ 0.005% — flat EMAs don't count as aligned
    ema5_arr = ind.ema5_series.iloc[-check:].values
    ema13_arr = ind.ema13_series.iloc[-check:].values
    ema21_arr = ind.ema21_series.iloc[-check:].values
    ema5_slope = float(ema5_arr[-1]) - float(ema5_arr[0])
    ema_spread_pct = (
        abs(float(ema5_arr[-1]) - float(ema21_arr[-1])) / price if price else 0
    )
    ema_has_spread = ema_spread_pct >= 0.00005  # 0.005% minimum spread
    c1 = ema_has_spread and (
        (int(np.sum(ema5_arr < ema13_arr)) >= 3)
        or (ema5_slope < 0 and int(np.sum(ema5_arr < ema13_arr)) >= 2)
    )
    conds["ema_aligned_down"] = c1
    if c1:
        met += 1
        parts.append("EMA выровнены вниз")

    # 2. Price high touched EMA(13) zone — ±0.07% (widened for OTC spread tolerance)
    ema13_zone = ind.ema13 * 0.9993
    c2 = any(float(high[-i]) >= ema13_zone for i in range(1, min(4, n)))
    conds["price_near_ema13"] = c2
    if c2:
        met += 1
        parts.append("Коснулась EMA13 сверху (±0.07%)")

    # 3. Candle closed BELOW EMA(13)
    c3 = close[-1] < ind.ema13
    conds["close_below_ema13"] = c3
    if c3:
        met += 1
        parts.append("Закрылась ниже EMA13")

    # 4. Bounce candle is bearish with body > 50% avg_body
    c4 = close[-1] < open_[-1] and abs(close[-1] - open_[-1]) > avg_body * 0.5
    conds["bounce_candle_bearish"] = c4
    if c4:
        met += 1
        parts.append("Медвежья свеча отскока")

    # 5. Real pullback: 3 of 4 previous candles bullish AND at least one closed above EMA13
    c5 = False
    if n >= 5:
        pb_window = range(2, min(6, n))
        pb_bullish = [
            close[-i] > open_[-i] or abs(close[-i] - open_[-i]) < avg_body * 0.5
            for i in pb_window
        ]
        pb_above_ema = any(float(close[-i]) > ind.ema13 for i in pb_window)
        c5 = sum(pb_bullish) >= 3 and pb_above_ema
    conds["real_pullback"] = c5
    if c5:
        met += 1
        parts.append("Откат к EMA13 (3/4 свечи бычьи, одна над EMA)")

    # 6. RSI(7) between 30 and 60
    c6 = 30 <= ind.rsi <= 60
    conds["rsi_ok"] = c6
    if c6:
        met += 1
        parts.append(f"RSI {ind.rsi:.0f} в норме")

    # 7. Stochastic %K < %D (relaxed: no strict cross required)
    c7 = ind.stoch_k < ind.stoch_d
    conds["stoch_turning_down"] = c7
    if c7:
        met += 1
        parts.append(f"Stoch K<D ({ind.stoch_k:.0f}<{ind.stoch_d:.0f})")

    # 8. Bounce candle shows conviction: body > 0.5× avg AND closes in lower 50% of range
    total_range = high[-1] - low[-1]
    c8 = (
        abs(close[-1] - open_[-1]) > avg_body * 0.5
        and total_range > 0
        and (high[-1] - close[-1]) / total_range > 0.5
    )
    conds["candle_conviction"] = c8
    if c8:
        met += 1
        parts.append("Свеча закрылась в нижней зоне диапазона")

    return met, met, parts, conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "ema_bounce", reason, extra or {})


"""
Strategy 3 — Level Bounce (1m levels + 15s entry)

STEP 1: Find levels on 1m candles (last 50 bars).
        Prices where candle highs/lows cluster within 0.03% = level.
        Touch count = how many pivots landed in the cluster.

STEP 2: Detect reaction on 15s candles.
        Price approaching 1m level + rejection candle pattern.

Need 4 of 6 conditions. Base confidence: 40 + (met-4)*10.
All 6 conditions are always evaluated and returned in debug.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet


@dataclass
class StrategyResult:
    direction: str
    confidence: float
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL = 6
_MIN_MET = 4
_CLUSTER_PCT = 0.0003  # 0.03% — cluster radius for grouping nearby pivots
_APPROACH_PCT = 0.0005  # 0.05% — price is "at level" within this distance


# ── 1m level detection ────────────────────────────────────────────────────────


def find_1m_levels(
    df1m: pd.DataFrame,
) -> tuple[list[tuple[float, int]], list[tuple[float, int]]]:
    """
    Scan last 50 1m candles for S/R levels.
    Returns (supports, resistances) as list of (price, touch_count) tuples,
    sorted by touch_count descending.
    """
    n = min(50, len(df1m))
    highs = df1m["high"].values[-n:].astype(float)
    lows = df1m["low"].values[-n:].astype(float)

    res_prices: list[float] = []
    for i in range(1, n - 1):
        if highs[i] >= highs[i - 1] and highs[i] >= highs[i + 1]:
            res_prices.append(highs[i])

    sup_prices: list[float] = []
    for i in range(1, n - 1):
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]:
            sup_prices.append(lows[i])

    def _cluster(prices: list[float]) -> list[tuple[float, int]]:
        if not prices:
            return []
        prices_s = sorted(prices)
        used = [False] * len(prices_s)
        out: list[tuple[float, int]] = []
        for i, p in enumerate(prices_s):
            if used[i]:
                continue
            group = [p]
            used[i] = True
            for j in range(i + 1, len(prices_s)):
                if not used[j] and abs(prices_s[j] - p) / p < _CLUSTER_PCT:
                    group.append(prices_s[j])
                    used[j] = True
            out.append((float(np.mean(group)), len(group)))
        out.sort(key=lambda x: x[1], reverse=True)
        return out

    return _cluster(sup_prices), _cluster(res_prices)


# ── Main entry point ──────────────────────────────────────────────────────────


def level_bounce_strategy(
    df: pd.DataFrame,  # 15s candles (entry timing)
    ind: Indicators,
    levels: LevelSet,  # kept for API compatibility
    df1m_ctx: pd.DataFrame | None = None,  # 1m candles (level detection)
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
) -> StrategyResult:
    if df1m_ctx is None or len(df1m_ctx) < 10:
        return _none("Нет 1m данных для уровней", {"early_reject": "no_1m_data"})

    n = len(df)
    if n < 6:
        return _none("Мало 15s данных", {"early_reject": "n<6"})

    if ind.atr_ratio < 0.30:
        return _none(
            "ATR мёртвый", {"early_reject": f"atr_ratio={round(ind.atr_ratio, 3)}<0.30"}
        )

    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    price = float(close[-1])
    avg_body = (
        float(np.mean(np.abs(close[-min(10, n) :] - open_[-min(10, n) :]))) or 1e-8
    )

    # STEP 1 — find 1m levels
    sup_levels, res_levels = find_1m_levels(df1m_ctx)

    # STEP 2 — evaluate each direction (always returns full condition set for debug)
    best_buy = _eval_buy(
        close,
        open_,
        high,
        low,
        n,
        price,
        avg_body,
        ind,
        sup_levels,
        res_levels,
        mode,
        ctx_trend_up,
    )
    best_sell = _eval_sell(
        close,
        open_,
        high,
        low,
        n,
        price,
        avg_body,
        ind,
        res_levels,
        sup_levels,
        mode,
        ctx_trend_down,
    )

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Уровень не найден или нет паттерна"

    if best_buy["met"] >= _MIN_MET and best_buy["conf"] > best_sell["conf"]:
        direction = "BUY"
        conditions_met = best_buy["met"]
        base_conf = best_buy["conf"]
        reason = best_buy["reason"]
    elif best_sell["met"] >= _MIN_MET and best_sell["conf"] > best_buy["conf"]:
        direction = "SELL"
        conditions_met = best_sell["met"]
        base_conf = best_sell["conf"]
        reason = best_sell["reason"]

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="level_bounce",
        reasoning=reason,
        debug={
            "buy_score": best_buy["conf"],
            "sell_score": best_sell["conf"],
            "buy_conditions": best_buy["conds"],
            "sell_conditions": best_sell["conds"],
            "buy_met": best_buy["met"],
            "sell_met": best_sell["met"],
            "sup_levels_1m": [(round(p, 5), t) for p, t in sup_levels[:5]],
            "res_levels_1m": [(round(p, 5), t) for p, t in res_levels[:5]],
        },
    )


# ── BUY: bounce from 1m support ───────────────────────────────────────────────


def _eval_buy(
    close,
    open_,
    high,
    low,
    n,
    price,
    avg_body,
    ind,
    sup_levels,
    res_levels,
    mode,
    ctx_confirms: bool = False,
) -> dict:
    """
    Evaluate all 6 conditions for every candidate support level.
    Always returns the best partial result (most conditions met),
    even if below _MIN_MET — so debug always shows condition checkmarks.
    ctx_confirms: True if 1m MTF trend agrees with BUY direction.
    """
    best: dict = {"met": 0, "conf": 0.0, "reason": "", "conds": {}}

    for sup_price, touch_count in sup_levels[:5]:
        if touch_count < 2:
            continue

        conds: dict[str, bool] = {}
        met = 0
        parts: list[str] = []

        # 1. Strong 1m level: 2+ touches
        c1 = True  # already filtered above
        conds["strong_1m_level"] = c1
        met += 1
        parts.append(f"Уровень 1m {sup_price:.5f} ({touch_count}x)")

        # 2. Price at level: close or low of last 4 15s candles within 0.05%
        c2 = any(
            abs(float(low[-i]) - sup_price) / sup_price < _APPROACH_PCT
            or abs(float(close[-i]) - sup_price) / sup_price < _APPROACH_PCT
            for i in range(1, min(5, n))
        )
        conds["price_at_level"] = c2
        if c2:
            met += 1
            parts.append("Цена у уровня (±0.05%)")

        # 3. Rejection candle 15s: lower wick > 2x body
        body = abs(float(close[-1]) - float(open_[-1]))
        wick_low = min(float(close[-1]), float(open_[-1])) - float(low[-1])
        c3 = (wick_low > 2.0 * body) if body > 1e-10 else (wick_low > avg_body * 0.5)
        conds["rejection_candle_15s"] = c3
        if c3:
            ratio = wick_low / avg_body if avg_body > 0 else 0
            met += 1
            parts.append(f"Отскок-свеча (тень {ratio:.1f}x avg)")

        # 4. RSI extreme: RSI < 40
        c4 = ind.rsi < 40
        conds["rsi_extreme_15s"] = c4
        if c4:
            met += 1
            parts.append(f"RSI перепродан ({ind.rsi:.0f})")

        # 5. Stoch turning up from low
        c5 = ind.stoch_k < 30 and ind.stoch_k > ind.stoch_k_prev
        conds["stoch_confirm"] = c5
        if c5:
            met += 1
            parts.append(f"Stoch разворот вверх ({ind.stoch_k:.0f})")

        # 6. Room to target: > 0.025% to nearest resistance
        nearest_res = res_levels[0][0] if res_levels else None
        room = (
            (nearest_res - price) / price * 100
            if (nearest_res and nearest_res > price)
            else 0.0
        )
        c6 = room > 0.025
        conds["room_to_target"] = c6
        if c6:
            met += 1
            parts.append(f"Пространство {room:.2f}%")

        # Compute confidence (only meaningful when met >= _MIN_MET)
        conf = 0.0
        if met >= _MIN_MET:
            conf = 40 + max(0, met - 4) * 10
            if touch_count >= 3:
                conf += 5
            if mode == "RANGE":
                conf += 5  # range bonus
            if ctx_confirms:
                conf += 5  # 1m MTF trend confirms direction
            # Precision level touch bonus: price within 0.01% of support → +10
            # Boosts level_bounce over conflicting ema_bounce when price is exactly at level
            dist_pct = min(
                abs(float(close[-1]) - sup_price) / sup_price,
                abs(float(low[-1]) - sup_price) / sup_price,
            )
            if dist_pct < 0.0001:  # < 0.01%
                conf += 10
                parts.append("Точное касание уровня (<0.01%) +10")

        # Always update best if this level has more conditions met
        # (so debug always shows full condition set, even below threshold)
        if met > best["met"] or (met >= _MIN_MET and conf > best["conf"]):
            best = {
                "met": met,
                "conf": conf,
                "reason": " | ".join(parts),
                "conds": conds,
            }

    return best


# ── SELL: bounce from 1m resistance ──────────────────────────────────────────


def _eval_sell(
    close,
    open_,
    high,
    low,
    n,
    price,
    avg_body,
    ind,
    res_levels,
    sup_levels,
    mode,
    ctx_confirms: bool = False,
) -> dict:
    """
    Evaluate all 6 conditions for every candidate resistance level.
    Always returns the best partial result for debug visibility.
    ctx_confirms: True if 1m MTF trend agrees with SELL direction.
    """
    best: dict = {"met": 0, "conf": 0.0, "reason": "", "conds": {}}

    for res_price, touch_count in res_levels[:5]:
        if touch_count < 2:
            continue

        conds: dict[str, bool] = {}
        met = 0
        parts: list[str] = []

        # 1. Strong 1m level
        c1 = True
        conds["strong_1m_level"] = c1
        met += 1
        parts.append(f"Сопротивление 1m {res_price:.5f} ({touch_count}x)")

        # 2. Price at level: close or high of last 4 15s candles within 0.05%
        c2 = any(
            abs(float(high[-i]) - res_price) / res_price < _APPROACH_PCT
            or abs(float(close[-i]) - res_price) / res_price < _APPROACH_PCT
            for i in range(1, min(5, n))
        )
        conds["price_at_level"] = c2
        if c2:
            met += 1
            parts.append("Цена у уровня (±0.05%)")

        # 3. Rejection candle 15s: upper wick > 2x body
        body = abs(float(close[-1]) - float(open_[-1]))
        wick_high = float(high[-1]) - max(float(close[-1]), float(open_[-1]))
        c3 = (wick_high > 2.0 * body) if body > 1e-10 else (wick_high > avg_body * 0.5)
        conds["rejection_candle_15s"] = c3
        if c3:
            ratio = wick_high / avg_body if avg_body > 0 else 0
            met += 1
            parts.append(f"Отскок-свеча (тень {ratio:.1f}x avg)")

        # 4. RSI extreme: RSI > 60
        c4 = ind.rsi > 60
        conds["rsi_extreme_15s"] = c4
        if c4:
            met += 1
            parts.append(f"RSI перекуплен ({ind.rsi:.0f})")

        # 5. Stoch turning down from high
        c5 = ind.stoch_k > 70 and ind.stoch_k < ind.stoch_k_prev
        conds["stoch_confirm"] = c5
        if c5:
            met += 1
            parts.append(f"Stoch разворот вниз ({ind.stoch_k:.0f})")

        # 6. Room to target: > 0.025% to nearest support
        nearest_sup = sup_levels[0][0] if sup_levels else None
        room = (
            (price - nearest_sup) / price * 100
            if (nearest_sup and nearest_sup < price)
            else 0.0
        )
        c6 = room > 0.025
        conds["room_to_target"] = c6
        if c6:
            met += 1
            parts.append(f"Пространство {room:.2f}%")

        conf = 0.0
        if met >= _MIN_MET:
            conf = 40 + max(0, met - 4) * 10
            if touch_count >= 3:
                conf += 5
            if mode == "RANGE":
                conf += 5  # range bonus
            if ctx_confirms:
                conf += 5  # 1m MTF trend confirms direction
            # Precision level touch bonus: price within 0.01% of resistance → +10
            dist_pct = min(
                abs(float(close[-1]) - res_price) / res_price,
                abs(float(high[-1]) - res_price) / res_price,
            )
            if dist_pct < 0.0001:  # < 0.01%
                conf += 10
                parts.append("Точное касание уровня (<0.01%) +10")

        if met > best["met"] or (met >= _MIN_MET and conf > best["conf"]):
            best = {
                "met": met,
                "conf": conf,
                "reason": " | ".join(parts),
                "conds": conds,
            }

    return best


# ── Helpers ───────────────────────────────────────────────────────────────────


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "level_bounce", reason, extra or {})


"""
Strategy 7 — Level Breakout (1m levels + 15s entry)

STEP 1: Find levels on 1m candles (need 3+ touches for breakout validity).
STEP 2: Detect breakout close on 15s candles + momentum confirmation.

Need 4 of 6 conditions. Base confidence: 40 + (met-4)*10.
All 6 conditions are always evaluated and returned in debug.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet
from .level_bounce import find_1m_levels


@dataclass
class StrategyResult:
    direction: str
    confidence: float
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL = 6
_MIN_MET = 4


def level_breakout_strategy(
    df: pd.DataFrame,  # 15s candles (entry timing)
    ind: Indicators,
    levels: LevelSet,  # kept for API compatibility
    df1m_ctx: pd.DataFrame | None = None,  # 1m candles (level detection)
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
) -> StrategyResult:
    if df1m_ctx is None or len(df1m_ctx) < 10:
        return _none("Нет 1m данных", {"early_reject": "no_1m_data"})

    n = len(df)
    if n < 6:
        return _none("Мало 15s данных", {"early_reject": "n<6"})

    if ind.atr_ratio < 0.40:
        return _none(
            "ATR слишком мал для пробоя",
            {"early_reject": f"atr_ratio={round(ind.atr_ratio, 3)}<0.40"},
        )

    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    price = float(close[-1])
    avg_body = (
        float(np.mean(np.abs(close[-min(10, n) :] - open_[-min(10, n) :]))) or 1e-8
    )

    sup_levels, res_levels = find_1m_levels(df1m_ctx)

    best_buy = _eval_breakout_buy(
        close, open_, high, low, n, price, avg_body, ind, res_levels, mode
    )
    best_sell = _eval_breakout_sell(
        close, open_, high, low, n, price, avg_body, ind, sup_levels, mode
    )

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Нет пробоя уровня"

    if best_buy["met"] >= _MIN_MET and best_buy["conf"] > best_sell["conf"]:
        direction = "BUY"
        conditions_met = best_buy["met"]
        base_conf = best_buy["conf"]
        reason = best_buy["reason"]
    elif best_sell["met"] >= _MIN_MET and best_sell["conf"] > best_buy["conf"]:
        direction = "SELL"
        conditions_met = best_sell["met"]
        base_conf = best_sell["conf"]
        reason = best_sell["reason"]

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="level_breakout",
        reasoning=reason,
        debug={
            "buy_score": best_buy["conf"],
            "sell_score": best_sell["conf"],
            "buy_conditions": best_buy["conds"],
            "sell_conditions": best_sell["conds"],
            "buy_met": best_buy["met"],
            "sell_met": best_sell["met"],
            "sup_levels_1m": [(round(p, 5), t) for p, t in sup_levels[:5]],
            "res_levels_1m": [(round(p, 5), t) for p, t in res_levels[:5]],
        },
    )


# ── BUY: breakout above 1m resistance ────────────────────────────────────────


def _eval_breakout_buy(
    close, open_, high, low, n, price, avg_body, ind, res_levels, mode
) -> dict:
    """
    Evaluate all 6 conditions for every resistance level with 3+ touches.
    Always returns best partial result for debug visibility.
    """
    best: dict = {"met": 0, "conf": 0.0, "reason": "", "conds": {}}

    for res_price, touch_count in res_levels[:5]:
        if touch_count < 3:
            continue

        conds: dict[str, bool] = {}
        met = 0
        parts: list[str] = []

        # 1. tested_level: 3+ touches on 1m chart
        c1 = True
        conds["tested_level"] = c1
        met += 1
        parts.append(f"Протестир. сопр. {res_price:.5f} ({touch_count}x)")

        # 2. close_beyond: 15s close ABOVE resistance
        c2 = float(close[-1]) > res_price
        conds["close_beyond"] = c2
        if c2:
            met += 1
            parts.append(f"Закрылась выше {res_price:.5f}")

        # 3. momentum_candle: bullish body > 1.5x avg
        body = abs(float(close[-1]) - float(open_[-1]))
        c3 = body > avg_body * 1.5 and float(close[-1]) > float(open_[-1])
        conds["momentum_candle"] = c3
        if c3:
            met += 1
            parts.append(f"Импульс (тело {body / avg_body:.1f}x avg)")

        # 4. follow_through: previous 15s candle also bullish
        c4 = n >= 2 and float(close[-2]) > float(open_[-2])
        conds["follow_through"] = c4
        if c4:
            met += 1
            parts.append("Продолжение (пред. свеча бычья)")

        # 5. ema_aligned: EMA5 > EMA13
        c5 = ind.ema5 > ind.ema13
        conds["ema_aligned"] = c5
        if c5:
            met += 1
            parts.append("EMA вверх")

        # 6. not_exhausted: RSI 35-65
        c6 = 35 <= ind.rsi <= 65
        conds["not_exhausted"] = c6
        if c6:
            met += 1
            parts.append(f"RSI в норме ({ind.rsi:.0f})")

        conf = 0.0
        if met >= _MIN_MET:
            conf = 40 + max(0, met - 4) * 10
            if touch_count >= 4:
                conf += 5

        if met > best["met"] or (met >= _MIN_MET and conf > best["conf"]):
            best = {
                "met": met,
                "conf": conf,
                "reason": " | ".join(parts),
                "conds": conds,
            }

    return best


# ── SELL: breakout below 1m support ──────────────────────────────────────────


def _eval_breakout_sell(
    close, open_, high, low, n, price, avg_body, ind, sup_levels, mode
) -> dict:
    """
    Evaluate all 6 conditions for every support level with 3+ touches.
    Always returns best partial result for debug visibility.
    """
    best: dict = {"met": 0, "conf": 0.0, "reason": "", "conds": {}}

    for sup_price, touch_count in sup_levels[:5]:
        if touch_count < 3:
            continue

        conds: dict[str, bool] = {}
        met = 0
        parts: list[str] = []

        # 1. tested_level: 3+ touches
        c1 = True
        conds["tested_level"] = c1
        met += 1
        parts.append(f"Протестир. пд. {sup_price:.5f} ({touch_count}x)")

        # 2. close_beyond: 15s close BELOW support
        c2 = float(close[-1]) < sup_price
        conds["close_beyond"] = c2
        if c2:
            met += 1
            parts.append(f"Закрылась ниже {sup_price:.5f}")

        # 3. momentum_candle: bearish body > 1.5x avg
        body = abs(float(close[-1]) - float(open_[-1]))
        c3 = body > avg_body * 1.5 and float(close[-1]) < float(open_[-1])
        conds["momentum_candle"] = c3
        if c3:
            met += 1
            parts.append(f"Импульс (тело {body / avg_body:.1f}x avg)")

        # 4. follow_through: previous 15s candle also bearish
        c4 = n >= 2 and float(close[-2]) < float(open_[-2])
        conds["follow_through"] = c4
        if c4:
            met += 1
            parts.append("Продолжение (пред. свеча медвежья)")

        # 5. ema_aligned: EMA5 < EMA13
        c5 = ind.ema5 < ind.ema13
        conds["ema_aligned"] = c5
        if c5:
            met += 1
            parts.append("EMA вниз")

        # 6. not_exhausted: RSI 35-65
        c6 = 35 <= ind.rsi <= 65
        conds["not_exhausted"] = c6
        if c6:
            met += 1
            parts.append(f"RSI в норме ({ind.rsi:.0f})")

        conf = 0.0
        if met >= _MIN_MET:
            conf = 40 + max(0, met - 4) * 10
            if touch_count >= 4:
                conf += 5

        if met > best["met"] or (met >= _MIN_MET and conf > best["conf"]):
            best = {
                "met": met,
                "conf": conf,
                "reason": " | ".join(parts),
                "conds": conds,
            }

    return best


# ── Helpers ───────────────────────────────────────────────────────────────────


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "level_breakout", reason, extra or {})


"""
Strategy 2 — Squeeze Breakout
Scenario: Market compressed (small candles, low ATR, narrow BB).
A large impulse candle breaks out — enter in breakout direction.
Best in: SQUEEZE mode (primary) and TRENDING modes (secondary).
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet


@dataclass
class StrategyResult:
    direction: str
    confidence: float
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL = 7


def squeeze_breakout_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
) -> StrategyResult:
    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    n = len(df)

    if n < 15:
        return _none("Мало данных", {"early_reject": "n<15"})

    # Squeeze breakouts need real energy to follow through
    if ind.atr_ratio < 0.50:
        return _none(
            "ATR слишком мал для пробоя",
            {
                "early_reject": f"atr_ratio={round(ind.atr_ratio, 3)}<0.50",
                "atr_ratio": round(ind.atr_ratio, 3),
            },
        )

    price = close[-1]
    avg_body_30 = (
        float(np.mean(np.abs(close[-min(30, n) :] - open_[-min(30, n) :]))) or 1e-8
    )
    avg_body_10 = (
        float(np.mean(np.abs(close[-min(10, n) :] - open_[-min(10, n) :]))) or 1e-8
    )
    curr_body = abs(close[-1] - open_[-1])

    # ── BUY / SELL check ───────────────────────────────────────────────────────
    buy_met, buy_parts, buy_conds = _check_buy(
        close, open_, high, low, n, ind, avg_body_30, avg_body_10, curr_body, levels
    )
    sell_met, sell_parts, sell_conds = _check_sell(
        close, open_, high, low, n, ind, avg_body_30, avg_body_10, curr_body, levels
    )

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Условия не выполнены"

    if buy_met > sell_met and buy_met >= 4:
        direction = "BUY"
        conditions_met = buy_met
        base_conf = buy_met / _TOTAL * 80
        reason = " | ".join(buy_parts)
        # Squeeze duration bonus — check if last 8+ candles were all small
        small_run = sum(
            1
            for i in range(1, min(9, n))
            if abs(close[-i] - open_[-i]) < avg_body_30 * 0.6
        )
        if small_run >= 8:
            base_conf += 5
        if close[-1] > ind.bb_upper:
            base_conf += 5  # broke BB
        if ind.momentum > ind.momentum_prev * 2 and ind.momentum > 0:
            base_conf += 3

    elif sell_met > buy_met and sell_met >= 4:
        direction = "SELL"
        conditions_met = sell_met
        base_conf = sell_met / _TOTAL * 80
        reason = " | ".join(sell_parts)
        small_run = sum(
            1
            for i in range(1, min(9, n))
            if abs(close[-i] - open_[-i]) < avg_body_30 * 0.6
        )
        if small_run >= 8:
            base_conf += 5
        if close[-1] < ind.bb_lower:
            base_conf += 5
        if ind.momentum < ind.momentum_prev * 2 and ind.momentum < 0:
            base_conf += 3

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="squeeze_breakout",
        reasoning=reason,
        debug={
            "buy_met": buy_met,
            "sell_met": sell_met,
            "avg_body_30": round(avg_body_30, 6),
            "buy_conditions": buy_conds,
            "sell_conditions": sell_conds,
        },
    )


def _check_buy(
    close,
    open_,
    high,
    low,
    n,
    ind: Indicators,
    avg_body_30,
    avg_body_10,
    curr_body,
    levels: LevelSet,
):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # 0. Exhaustion guard — RSI < 70 AND Stoch K < 85
    #    If already overbought the breakout move is exhausted; skip immediately.
    c0 = ind.rsi < 70 and ind.stoch_k < 85
    conds["not_exhausted"] = c0
    if not c0:
        parts.append(
            f"Перекупленность (RSI={ind.rsi:.1f}, StochK={ind.stoch_k:.1f}) — пропуск"
        )
        return 0, parts, conds

    # 1. ATR compressed — relaxed: < 0.85× avg30 (was 0.70×)
    c1 = ind.atr_ratio < 0.85
    conds["atr_compressed"] = c1
    if c1:
        met += 1
        parts.append(f"ATR сжат ×{ind.atr_ratio:.2f}")

    # 2. Small candles — relaxed: 4+ of 8 bodies < 75% of avg_30 (was 5+, threshold 60%)
    small_recent = sum(
        1
        for i in range(1, min(9, n))
        if abs(close[-i] - open_[-i]) < avg_body_30 * 0.75
    )
    c2 = small_recent >= 4
    conds["small_candles_compressed"] = c2
    if c2:
        met += 1
        parts.append(f"Сжатые свечи ({small_recent} из последних 8)")

    # 3. Current candle body > 1.5× avg body of last 10 and bullish (relaxed from 2.0×)
    c3 = curr_body > avg_body_10 * 1.5 and close[-1] > open_[-1]
    conds["breakout_candle_bullish"] = c3
    if c3:
        met += 1
        parts.append(f"Пробойная бычья свеча (×{curr_body / avg_body_10:.1f})")

    # 4. Close > upper BB or > highest high of last 8 candles
    high_8 = float(max(high[-min(9, n) : -1])) if n >= 2 else float(high[-1])
    c4 = close[-1] > ind.bb_upper or close[-1] > high_8
    conds["breaks_bb_or_range"] = c4
    if c4:
        met += 1
        parts.append("Пробой вверх (BB/диапазон)")

    # 5. Shadow against direction < 30% of candle range
    total_range = high[-1] - low[-1]
    upper_shadow = high[-1] - max(close[-1], open_[-1])
    c5 = total_range > 0 and upper_shadow / total_range < 0.3
    conds["small_upper_shadow"] = c5
    if c5:
        met += 1
        parts.append("Малая верхняя тень")

    # 6. Momentum > 0 and positive
    c6 = ind.momentum > 0
    conds["momentum_positive"] = c6
    if c6:
        met += 1
        parts.append(f"Моментум растёт ({ind.momentum:+.5f})")

    # 7. EMA(5) turning up or already > EMA(13)
    c7 = ind.ema5 >= ind.ema13 or ind.ema5 > float(ind.ema5_series.iloc[-2])
    conds["ema5_turning_up"] = c7
    if c7:
        met += 1
        parts.append("EMA5 поворачивает вверх")

    # Penalty: shadow > 60% of body or running into strong resistance
    upper_shadow_body = upper_shadow / (curr_body + 1e-10)
    if upper_shadow_body > 0.6:
        met = max(0, met - 2)  # heavy penalty
    if levels.dist_to_res_pct < 0.05:
        met = max(0, met - 2)

    return met, parts, conds


def _check_sell(
    close,
    open_,
    high,
    low,
    n,
    ind: Indicators,
    avg_body_30,
    avg_body_10,
    curr_body,
    levels: LevelSet,
):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # 0. Exhaustion guard — RSI > 30 AND Stoch K > 15
    #    If already oversold the breakdown move is exhausted; skip immediately.
    c0 = ind.rsi > 30 and ind.stoch_k > 15
    conds["not_exhausted"] = c0
    if not c0:
        parts.append(
            f"Перепроданность (RSI={ind.rsi:.1f}, StochK={ind.stoch_k:.1f}) — пропуск"
        )
        return 0, parts, conds

    # 1. ATR compressed — relaxed: < 0.85× avg30 (was 0.70×)
    c1 = ind.atr_ratio < 0.85
    conds["atr_compressed"] = c1
    if c1:
        met += 1
        parts.append(f"ATR сжат ×{ind.atr_ratio:.2f}")

    # 2. Small candles — relaxed: 4+ of 8 bodies < 75% of avg_30 (was 5+, threshold 60%)
    small_recent = sum(
        1
        for i in range(1, min(9, n))
        if abs(close[-i] - open_[-i]) < avg_body_30 * 0.75
    )
    c2 = small_recent >= 4
    conds["small_candles_compressed"] = c2
    if c2:
        met += 1
        parts.append(f"Сжатые свечи ({small_recent})")

    # 3. Current candle body > 1.5× avg body of last 10 and bearish (relaxed from 2.0×)
    c3 = curr_body > avg_body_10 * 1.5 and close[-1] < open_[-1]
    conds["breakout_candle_bearish"] = c3
    if c3:
        met += 1
        parts.append(f"Пробойная медвежья свеча (×{curr_body / avg_body_10:.1f})")

    low_8 = float(min(low[-min(9, n) : -1])) if n >= 2 else float(low[-1])
    c4 = close[-1] < ind.bb_lower or close[-1] < low_8
    conds["breaks_bb_or_range"] = c4
    if c4:
        met += 1
        parts.append("Пробой вниз (BB/диапазон)")

    total_range = high[-1] - low[-1]
    lower_shadow = min(close[-1], open_[-1]) - low[-1]
    c5 = total_range > 0 and lower_shadow / total_range < 0.3
    conds["small_lower_shadow"] = c5
    if c5:
        met += 1
        parts.append("Малая нижняя тень")

    c6 = ind.momentum < 0
    conds["momentum_negative"] = c6
    if c6:
        met += 1
        parts.append(f"Моментум падает ({ind.momentum:+.5f})")

    c7 = ind.ema5 <= ind.ema13 or ind.ema5 < float(ind.ema5_series.iloc[-2])
    conds["ema5_turning_down"] = c7
    if c7:
        met += 1
        parts.append("EMA5 поворачивает вниз")

    lower_shadow_body = lower_shadow / (curr_body + 1e-10)
    if lower_shadow_body > 0.6:  # relaxed from 0.5
        met = max(0, met - 2)
    if levels.dist_to_sup_pct < 0.05:
        met = max(0, met - 2)

    return met, parts, conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult(
        "NONE", 0.0, 0, _TOTAL, "squeeze_breakout", reason, extra or {}
    )


"""
Strategy 4 — RSI Extreme Reversal
Scenario: Strong one-directional move pushed RSI to extreme (<25 or >75).
Multiple confirming conditions required before signaling a snapback.
Best in: RANGE, VOLATILE modes.

8 conditions, minimum 5 required.
Confidence = (conditions_met / 8) * 85 + small bonuses (capped +12).
NO 5-min trend dependency — reversals are by definition counter-trend.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet
from ..candle_patterns import detect_reversal_pattern


@dataclass
class StrategyResult:
    direction: str
    confidence: float
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL = 8
_MIN_MET = 5


def rsi_reversal_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
) -> StrategyResult:
    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    n = len(df)

    if n < 8:
        return _none("Мало данных")

    # Hard reject: dead market — reversals need energy
    if ind.atr_ratio < 0.4:
        return _none("ATR мёртвый — рынок стоит")

    # Hard reject: RSI not extreme enough
    if 25 <= ind.rsi <= 75:
        return _none(f"RSI {ind.rsi:.0f} не в экстремуме (нужно <25 или >75)")

    avg_body = (
        float(np.mean(np.abs(close[-min(10, n) :] - open_[-min(10, n) :]))) or 1e-8
    )
    avg_range = float(np.mean(high[-min(10, n) :] - low[-min(10, n) :])) or 1e-8

    if ind.rsi < 25:
        # Hard reject: price at resistance — no room to go up
        if levels.dist_to_res_pct < 0.05:
            return _none("Цена у сопротивления — нет места для роста")
        met, conf, parts = _check_buy(
            close, open_, high, low, n, ind, levels, avg_body, avg_range
        )
        direction = "BUY"
    else:
        # Hard reject: price at support — no room to go down
        if levels.dist_to_sup_pct < 0.05:
            return _none("Цена у поддержки — нет места для падения")
        met, conf, parts = _check_sell(
            close, open_, high, low, n, ind, levels, avg_body, avg_range
        )
        direction = "SELL"

    if met < _MIN_MET:
        return _none(f"Только {met}/{_TOTAL} условий — нужно минимум {_MIN_MET}")

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, conf),
        conditions_met=met,
        total_conditions=_TOTAL,
        strategy_name="rsi_reversal",
        reasoning="RSI разворот: " + " | ".join(parts),
        debug={
            "rsi": round(ind.rsi, 1),
            "rsi_prev": round(ind.rsi_prev, 1),
            "met": met,
            "conf": round(conf, 1),
        },
    )


def _check_buy(
    close,
    open_,
    high,
    low,
    n,
    ind: Indicators,
    levels: LevelSet,
    avg_body: float,
    avg_range: float,
):
    """RSI snapback from oversold → BUY. 8 conditions, need 5."""
    met = 0
    parts = []

    # CONDITION 1 — RSI extreme (< 25) — entry ticket
    # Guaranteed by hard reject above; always counts.
    met += 1
    parts.append(f"RSI перепродан ({ind.rsi:.0f})")

    # CONDITION 2 — RSI genuinely turning up (real previous RSI, not 50.0)
    if ind.rsi > ind.rsi_prev and ind.rsi_prev < 30:
        met += 1
        parts.append(f"RSI разворачивается вверх ({ind.rsi_prev:.0f}→{ind.rsi:.0f})")

    # CONDITION 3 — Bearish run before reversal (3+ red candles among last 5)
    bear_count = sum(1 for i in range(2, min(7, n)) if close[-i] < open_[-i])
    if bear_count >= 3:
        met += 1
        parts.append(f"Медвежий забег ({bear_count} свечей)")

    # CONDITION 4 — Reversal candle pattern (any one of three forms)
    curr_body = abs(close[-1] - open_[-1])
    lower_shadow = min(close[-1], open_[-1]) - low[-1]
    pat = detect_reversal_pattern(
        open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bull"
    )
    reversal_candle = False
    pat_label = ""

    if close[-1] > open_[-1] and curr_body > avg_body * 0.8:
        reversal_candle = True
        pat_label = f"Бычья свеча (тело ×{curr_body / avg_body:.1f})"
    elif lower_shadow > curr_body * 2.5 and lower_shadow > avg_body:
        reversal_candle = True
        pat_label = f"Пин-бар снизу (тень ×{lower_shadow / (curr_body + 1e-10):.1f})"
    elif pat.pattern == "engulfing":
        reversal_candle = True
        pat_label = "Бычье поглощение"

    if reversal_candle:
        met += 1
        parts.append(pat_label)

    # CONDITION 5 — Stochastic: both K and D oversold, K turning up
    if ind.stoch_k < 25 and ind.stoch_d < 25 and ind.stoch_k > ind.stoch_k_prev:
        met += 1
        parts.append(
            f"Stoch перепродан и разворачивается ({ind.stoch_k:.0f}/{ind.stoch_d:.0f})"
        )

    # CONDITION 6 — Price near support level
    if levels.nearest_sup > 0 and levels.dist_to_sup_pct < 0.15:
        met += 1
        parts.append(f"Рядом поддержка ({levels.dist_to_sup_pct:.2f}%)")

    # CONDITION 7 — Momentum shift: was negative, now rising
    if ind.momentum > ind.momentum_prev and ind.momentum_prev < 0:
        met += 1
        parts.append(
            f"Моментум разворачивается ({ind.momentum_prev:+.5f}→{ind.momentum:+.5f})"
        )

    # CONDITION 8 — Reversal candle has meaningful range (not a tiny doji)
    curr_range = high[-1] - low[-1]
    if curr_range > avg_range * 0.8:
        met += 1
        parts.append(f"Хороший размах свечи (×{curr_range / avg_range:.1f})")

    # Confidence: primarily driven by conditions_met
    base_conf = (met / _TOTAL) * 85

    # Small bonuses, capped at +12 total
    bonus = 0.0
    if ind.rsi < 15:
        bonus += 5
    elif ind.rsi < 20:
        bonus += 3
    if pat.pattern == "engulfing":
        bonus += 4
    elif pat.pattern == "pin_bar":
        bonus += 3
    if levels.nearest_sup > 0 and levels.dist_to_sup_pct < 0.05:
        bonus += 3
    bonus = min(bonus, 12.0)

    return met, base_conf + bonus, parts


def _check_sell(
    close,
    open_,
    high,
    low,
    n,
    ind: Indicators,
    levels: LevelSet,
    avg_body: float,
    avg_range: float,
):
    """RSI snapback from overbought → SELL. 8 conditions, need 5."""
    met = 0
    parts = []

    # CONDITION 1 — RSI extreme (> 75) — entry ticket
    met += 1
    parts.append(f"RSI перекуплен ({ind.rsi:.0f})")

    # CONDITION 2 — RSI genuinely turning down (real previous RSI)
    if ind.rsi < ind.rsi_prev and ind.rsi_prev > 70:
        met += 1
        parts.append(f"RSI разворачивается вниз ({ind.rsi_prev:.0f}→{ind.rsi:.0f})")

    # CONDITION 3 — Bullish run before reversal (3+ green candles among last 5)
    bull_count = sum(1 for i in range(2, min(7, n)) if close[-i] > open_[-i])
    if bull_count >= 3:
        met += 1
        parts.append(f"Бычий забег ({bull_count} свечей)")

    # CONDITION 4 — Reversal candle pattern
    curr_body = abs(close[-1] - open_[-1])
    upper_shadow = high[-1] - max(close[-1], open_[-1])
    pat = detect_reversal_pattern(
        open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bear"
    )
    reversal_candle = False
    pat_label = ""

    if close[-1] < open_[-1] and curr_body > avg_body * 0.8:
        reversal_candle = True
        pat_label = f"Медвежья свеча (тело ×{curr_body / avg_body:.1f})"
    elif upper_shadow > curr_body * 2.5 and upper_shadow > avg_body:
        reversal_candle = True
        pat_label = f"Пин-бар сверху (тень ×{upper_shadow / (curr_body + 1e-10):.1f})"
    elif pat.pattern == "engulfing":
        reversal_candle = True
        pat_label = "Медвежье поглощение"

    if reversal_candle:
        met += 1
        parts.append(pat_label)

    # CONDITION 5 — Stochastic: both K and D overbought, K turning down
    if ind.stoch_k > 75 and ind.stoch_d > 75 and ind.stoch_k < ind.stoch_k_prev:
        met += 1
        parts.append(
            f"Stoch перекуплен и разворачивается ({ind.stoch_k:.0f}/{ind.stoch_d:.0f})"
        )

    # CONDITION 6 — Price near resistance level
    if levels.nearest_res > 0 and levels.dist_to_res_pct < 0.15:
        met += 1
        parts.append(f"Рядом сопротивление ({levels.dist_to_res_pct:.2f}%)")

    # CONDITION 7 — Momentum shift: was positive, now falling
    if ind.momentum < ind.momentum_prev and ind.momentum_prev > 0:
        met += 1
        parts.append(
            f"Моментум разворачивается ({ind.momentum_prev:+.5f}→{ind.momentum:+.5f})"
        )

    # CONDITION 8 — Reversal candle has meaningful range
    curr_range = high[-1] - low[-1]
    if curr_range > avg_range * 0.8:
        met += 1
        parts.append(f"Хороший размах свечи (×{curr_range / avg_range:.1f})")

    # Confidence: primarily driven by conditions_met
    base_conf = (met / _TOTAL) * 85

    bonus = 0.0
    if ind.rsi > 85:
        bonus += 5
    elif ind.rsi > 80:
        bonus += 3
    if pat.pattern == "engulfing":
        bonus += 4
    elif pat.pattern == "pin_bar":
        bonus += 3
    if levels.nearest_res > 0 and levels.dist_to_res_pct < 0.05:
        bonus += 3
    bonus = min(bonus, 12.0)

    return met, base_conf + bonus, parts


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "rsi_reversal", reason, {})


"""
Strategy 5 — Micro Level Breakout
Scenario: Price tested a level 2-3 times and finally breaks through with a strong candle.
Breakout momentum carries 2-5 more candles.
Best in: VOLATILE mode and RANGE mode (when a level breaks).
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet


@dataclass
class StrategyResult:
    direction: str
    confidence: float
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL = 7


def micro_breakout_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
) -> StrategyResult:
    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    n = len(df)

    if n < 10:
        return _none("Мало данных")

    # Hard reject: micro breakouts need strong VOLATILE conditions
    if ind.atr_ratio < 0.55:
        return _none("ATR недостаточный для микропробоя")

    avg_body_10 = (
        float(np.mean(np.abs(close[-min(10, n) :] - open_[-min(10, n) :]))) or 1e-8
    )

    buy_met, buy_conf, buy_parts, buy_conds = _check_buy(
        close, open_, high, low, n, ind, levels, avg_body_10, ctx_trend_up
    )
    sell_met, sell_conf, sell_parts, sell_conds = _check_sell(
        close, open_, high, low, n, ind, levels, avg_body_10, ctx_trend_down
    )

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Уровень не пробит"

    if buy_conf > sell_conf and buy_met >= 4:
        direction = "BUY"
        conditions_met = buy_met
        base_conf = buy_conf
        reason = " | ".join(buy_parts)
    elif sell_conf > buy_conf and sell_met >= 4:
        direction = "SELL"
        conditions_met = sell_met
        base_conf = sell_conf
        reason = " | ".join(sell_parts)

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="micro_breakout",
        reasoning=reason,
        debug={
            "buy_met": buy_met,
            "sell_met": sell_met,
            "buy_conditions": buy_conds,
            "sell_conditions": sell_conds,
        },
    )


def _check_buy(
    close, open_, high, low, n, ind: Indicators, levels: LevelSet, avg_body_10, ctx_up
):
    """Breakout of resistance → BUY."""
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # Find a resistance level with 2+ touches that current candle broke
    broken_res = None
    touch_count = 0
    for res in levels.strong_res:
        if close[-1] > res:  # closed above resistance
            broken_res = res
            touch_count = 2
            break

    conds["resistance_found"] = broken_res is not None
    if broken_res is None:
        return 0, 0.0, [], conds

    met += 1
    parts.append(f"Сопротивление {broken_res:.5f} найдено (2+ касания)")
    conds["closed_above_resistance"] = True

    # 2. Current candle closed ABOVE resistance (always true if broken_res found)
    met += 1
    parts.append(f"Пробой: закрылась выше {broken_res:.5f}")

    curr_body = abs(close[-1] - open_[-1])
    upper_shadow = high[-1] - max(close[-1], open_[-1])

    # 3. Breakout candle body > 1.5× avg body
    c3 = curr_body > avg_body_10 * 1.5
    conds["strong_body"] = c3
    if c3:
        met += 1
        parts.append(f"Мощное тело ×{curr_body / avg_body_10:.1f}")

    # 4. Shadow against direction < 30% of body
    c4 = upper_shadow / (curr_body + 1e-10) < 0.3
    conds["small_upper_shadow"] = c4
    if c4:
        met += 1
        parts.append("Малая верхняя тень")

    # 5. Momentum > 0
    c5 = ind.momentum > 0
    conds["momentum_positive"] = c5
    if c5:
        met += 1
        parts.append("Моментум положительный")

    # 6. ATR active
    c6 = ind.atr_ratio > 0.9
    conds["atr_active"] = c6
    if c6:
        met += 1
        parts.append(f"ATR активный ×{ind.atr_ratio:.2f}")

    # 7. EMA pointing up
    c7a = ind.ema5 >= ind.ema13 and ind.ema13 >= ind.ema21
    c7b = ind.ema5 > float(ind.ema5_series.iloc[-3])
    conds["ema_pointing_up"] = c7a or c7b
    if c7a:
        met += 1
        parts.append("EMA вверх")
    elif c7b:
        met += 1
        parts.append("EMA5 поворачивает вверх")

    # Penalties (applied before base_conf calculation)
    another_res = [
        r
        for r in levels.resistances
        if r > broken_res and (r - close[-1]) / close[-1] < 0.0005
    ]
    conds["shadow_penalty"] = upper_shadow / (curr_body + 1e-10) > 0.5
    conds["wall_above"] = bool(another_res)
    conds["atr_weak"] = ind.atr_ratio < 0.6

    if conds["shadow_penalty"]:
        met = max(0, met - 2)
    if conds["wall_above"]:
        met = max(0, met - 2)
    if conds["atr_weak"]:
        met = max(0, met - 1)

    # base_conf calculated AFTER penalties
    base_conf = met / _TOTAL * 80

    # Bonuses
    if touch_count >= 3:
        base_conf += 5
    if (close[-1] - broken_res) / broken_res > 0.0005:
        base_conf += 3

    # Confidence penalty multipliers (applied after base_conf)
    if conds["shadow_penalty"]:
        base_conf *= 0.6
    if conds["wall_above"]:
        base_conf *= 0.6

    return met, base_conf, parts, conds


def _check_sell(
    close, open_, high, low, n, ind: Indicators, levels: LevelSet, avg_body_10, ctx_down
):
    """Breakout of support → SELL."""
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    broken_sup = None
    touch_count = 0
    for sup in levels.strong_sup:
        if close[-1] < sup:  # closed below support
            broken_sup = sup
            touch_count = 2
            break

    conds["support_found"] = broken_sup is not None
    if broken_sup is None:
        return 0, 0.0, [], conds

    met += 1
    parts.append(f"Поддержка {broken_sup:.5f} найдена (2+ касания)")
    conds["closed_below_support"] = True

    # 2. Current candle closed BELOW support (always true if broken_sup found)
    met += 1
    parts.append(f"Пробой: закрылась ниже {broken_sup:.5f}")

    curr_body = abs(close[-1] - open_[-1])
    lower_shadow = min(close[-1], open_[-1]) - low[-1]

    # 3. Breakout candle body > 1.5× avg body
    c3 = curr_body > avg_body_10 * 1.5
    conds["strong_body"] = c3
    if c3:
        met += 1
        parts.append(f"Мощное тело ×{curr_body / avg_body_10:.1f}")

    # 4. Shadow against direction < 30% of body
    c4 = lower_shadow / (curr_body + 1e-10) < 0.3
    conds["small_lower_shadow"] = c4
    if c4:
        met += 1
        parts.append("Малая нижняя тень")

    # 5. Momentum < 0
    c5 = ind.momentum < 0
    conds["momentum_negative"] = c5
    if c5:
        met += 1
        parts.append("Моментум отрицательный")

    # 6. ATR active
    c6 = ind.atr_ratio > 0.9
    conds["atr_active"] = c6
    if c6:
        met += 1
        parts.append(f"ATR активный ×{ind.atr_ratio:.2f}")

    # 7. EMA pointing down
    c7a = ind.ema5 <= ind.ema13 and ind.ema13 <= ind.ema21
    c7b = ind.ema5 < float(ind.ema5_series.iloc[-3])
    conds["ema_pointing_down"] = c7a or c7b
    if c7a:
        met += 1
        parts.append("EMA вниз")
    elif c7b:
        met += 1
        parts.append("EMA5 поворачивает вниз")

    # Penalties (applied before base_conf calculation)
    another_sup = [
        s
        for s in levels.supports
        if s < broken_sup and (close[-1] - s) / close[-1] < 0.0005
    ]
    conds["shadow_penalty"] = lower_shadow / (curr_body + 1e-10) > 0.5
    conds["wall_below"] = bool(another_sup)
    conds["atr_weak"] = ind.atr_ratio < 0.6

    if conds["shadow_penalty"]:
        met = max(0, met - 2)
    if conds["wall_below"]:
        met = max(0, met - 2)
    if conds["atr_weak"]:
        met = max(0, met - 1)

    # base_conf calculated AFTER penalties
    base_conf = met / _TOTAL * 80

    # Bonuses
    if touch_count >= 3:
        base_conf += 5
    if (broken_sup - close[-1]) / broken_sup > 0.0005:
        base_conf += 3

    # Confidence penalty multipliers (applied after base_conf)
    if conds["shadow_penalty"]:
        base_conf *= 0.6
    if conds["wall_below"]:
        base_conf *= 0.6

    return met, base_conf, parts, conds


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "micro_breakout", reason, {})


"""
Strategy 6 — Micro Divergence
Scenario: Price makes new high/low but RSI does not confirm → exhaustion → snapback.
Best in: RANGE, VOLATILE modes.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet
from ..candle_patterns import detect_reversal_pattern


@dataclass
class StrategyResult:
    direction: str
    confidence: float
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL = 5


def divergence_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
) -> StrategyResult:
    close = df["close"].values
    open_ = df["open"].values
    high = df["high"].values
    low = df["low"].values
    n = len(df)

    if n < 15:
        return _none("Мало данных для дивергенции", {"early_reject": "n<15"})

    # Divergence works in moderate conditions — lower bar than breakouts
    if ind.atr_ratio < 0.35:
        return _none(
            "ATR мёртвый — рынок стоит",
            {
                "early_reject": f"atr_ratio={round(ind.atr_ratio, 3)}<0.35",
                "atr_ratio": round(ind.atr_ratio, 3),
            },
        )

    avg_body = (
        float(np.mean(np.abs(close[-min(10, n) :] - open_[-min(10, n) :]))) or 1e-8
    )

    buy_met, buy_conf, buy_parts, buy_conds = _check_bullish_div(
        close, open_, high, low, n, ind, levels, avg_body, ctx_trend_up, mode
    )
    sell_met, sell_conf, sell_parts, sell_conds = _check_bearish_div(
        close, open_, high, low, n, ind, levels, avg_body, ctx_trend_down, mode
    )

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Дивергенция не обнаружена"

    if buy_conf > sell_conf and buy_met >= 4:
        direction = "BUY"
        conditions_met = buy_met
        base_conf = buy_conf
        reason = " | ".join(buy_parts)
    elif sell_conf > buy_conf and sell_met >= 4:
        direction = "SELL"
        conditions_met = sell_met
        base_conf = sell_conf
        reason = " | ".join(sell_parts)

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="divergence",
        reasoning=reason,
        debug={
            "buy_met": buy_met,
            "sell_met": sell_met,
            "rsi": round(ind.rsi, 1),
            "buy_conditions": buy_conds,
            "sell_conditions": sell_conds,
        },
    )


def _find_two_lows(
    low: np.ndarray, n: int, lookback: int = 20
) -> tuple[int, int] | None:
    """Find two local minima within lookback bars. Returns (idx1, idx2) with idx2 > idx1."""
    scan = low[-lookback:]
    m = len(scan)
    lows_idx = []
    for i in range(1, m - 1):
        if scan[i] <= scan[i - 1] and scan[i] <= scan[i + 1]:
            lows_idx.append(i)
    if len(lows_idx) < 2:
        return None
    # Take last two
    i1, i2 = lows_idx[-2], lows_idx[-1]
    if scan[i2] < scan[i1]:  # second low is lower — needed for divergence
        return (i1, i2)
    return None


def _find_two_highs(
    high: np.ndarray, n: int, lookback: int = 20
) -> tuple[int, int] | None:
    """Find two local maxima. Returns (idx1, idx2) with idx2 > idx1."""
    scan = high[-lookback:]
    m = len(scan)
    highs_idx = []
    for i in range(1, m - 1):
        if scan[i] >= scan[i - 1] and scan[i] >= scan[i + 1]:
            highs_idx.append(i)
    if len(highs_idx) < 2:
        return None
    i1, i2 = highs_idx[-2], highs_idx[-1]
    if scan[i2] > scan[i1]:  # second high is higher
        return (i1, i2)
    return None


def _approx_rsi_at(
    close: np.ndarray, idx: int, lookback: int, period: int = 7
) -> float:
    """Approximate RSI at a given index using a small window."""
    start = max(0, idx - period * 2)
    sub = pd.Series(close[start : idx + 1])
    delta = sub.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    ag = gain.ewm(com=period - 1, min_periods=period).mean()
    al = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = ag / al.replace(0, np.nan)
    r = 100 - (100 / (1 + rs))
    v = float(r.iloc[-1])
    return v if not np.isnan(v) else 50.0


def _check_bullish_div(close, open_, high, low, n, ind, levels, avg_body, ctx_up, mode):
    """Bullish divergence: price makes lower low, RSI makes higher low → BUY."""
    conds: dict[str, bool] = {}
    lookback = min(20, n - 1)
    pair = _find_two_lows(low, n, lookback)

    conds["two_lows_found"] = pair is not None
    if pair is None:
        return 0, 0.0, [], conds

    i1, i2 = pair
    abs_i1 = n - lookback + i1
    abs_i2 = n - lookback + i2

    low1 = low[abs_i1]
    low2 = low[abs_i2]
    conds["second_low_is_lower"] = low2 < low1
    if low2 >= low1:
        return 0, 0.0, [], conds

    rsi1 = _approx_rsi_at(close, abs_i1, lookback)
    rsi2 = _approx_rsi_at(close, abs_i2, lookback)

    rsi_diff = rsi2 - rsi1
    conds["rsi_higher_at_lower_price"] = rsi2 > rsi1
    conds["rsi_diff_significant"] = rsi_diff >= 5

    # Score ALL conditions — no early return here
    met = 0
    parts = []

    # 1. Two price lows, second lower — already verified above (pre-check)
    met += 1
    parts.append(f"Бычья дивергенция: цена {low2:.5f} < {low1:.5f}")

    # 2. RSI at second low > RSI at first low
    if rsi2 > rsi1:
        met += 1
        parts.append(f"RSI: {rsi2:.0f} > {rsi1:.0f} (рост при падении цены)")

    # 3. RSI difference > 5
    if rsi_diff >= 5:
        met += 1
        parts.append(f"Разница RSI {rsi_diff:.0f}pts — значимая")

    # 4. Bullish candle appeared after second low
    c4_bull = n > abs_i2 and close[-1] > open_[-1]
    c4_shadow = False
    if not c4_bull and n > abs_i2:
        lower_shadow = min(close[-1], open_[-1]) - low[-1]
        c4_shadow = lower_shadow > abs(close[-1] - open_[-1]) * 1.5
    conds["bullish_candle_or_shadow"] = c4_bull or c4_shadow
    if c4_bull:
        met += 1
        parts.append("Бычья свеча после минимума")
    elif c4_shadow:
        met += 1
        parts.append("Нижняя тень (бычье давление)")

    # 5. Stochastic turning up
    c5 = ind.stoch_k > ind.stoch_k_prev
    conds["stoch_turning_up"] = c5
    if c5:
        met += 1
        parts.append(f"Stoch поворачивает вверх ({ind.stoch_k:.0f})")

    # Confidence: conditions-driven (not hardcoded)
    base_conf = (met / _TOTAL) * 85
    if _divergence_at_level(low2, levels.supports):
        base_conf += 7
    if abs(rsi_diff) > 10:
        base_conf += 5
    if mode == "RANGE":
        base_conf += 3

    return met, base_conf, parts, conds


def _check_bearish_div(
    close, open_, high, low, n, ind, levels, avg_body, ctx_down, mode
):
    """Bearish divergence: price makes higher high, RSI makes lower high → SELL."""
    conds: dict[str, bool] = {}
    lookback = min(20, n - 1)
    pair = _find_two_highs(high, n, lookback)

    conds["two_highs_found"] = pair is not None
    if pair is None:
        return 0, 0.0, [], conds

    i1, i2 = pair
    abs_i1 = n - lookback + i1
    abs_i2 = n - lookback + i2

    high1 = high[abs_i1]
    high2 = high[abs_i2]
    conds["second_high_is_higher"] = high2 > high1
    if high2 <= high1:
        return 0, 0.0, [], conds

    rsi1 = _approx_rsi_at(close, abs_i1, lookback)
    rsi2 = _approx_rsi_at(close, abs_i2, lookback)

    rsi_diff = rsi1 - rsi2  # rsi2 < rsi1 at higher price = bearish divergence
    conds["rsi_lower_at_higher_price"] = rsi2 < rsi1
    conds["rsi_diff_significant"] = rsi_diff >= 5

    # Score ALL conditions — no early return here
    met = 0
    parts = []

    # 1. Two price highs, second higher — already verified above (pre-check)
    met += 1
    parts.append(f"Медвежья дивергенция: цена {high2:.5f} > {high1:.5f}")

    # 2. RSI at second high < RSI at first high
    if rsi2 < rsi1:
        met += 1
        parts.append(f"RSI: {rsi2:.0f} < {rsi1:.0f} (падение при росте цены)")

    # 3. RSI difference > 5
    if rsi_diff >= 5:
        met += 1
        parts.append(f"Разница RSI {rsi_diff:.0f}pts — значимая")

    c4_bear = n > abs_i2 and close[-1] < open_[-1]
    c4_shadow = False
    if not c4_bear and n > abs_i2:
        upper_shadow = high[-1] - max(close[-1], open_[-1])
        c4_shadow = upper_shadow > abs(close[-1] - open_[-1]) * 1.5
    conds["bearish_candle_or_shadow"] = c4_bear or c4_shadow
    if c4_bear:
        met += 1
        parts.append("Медвежья свеча после максимума")
    elif c4_shadow:
        met += 1
        parts.append("Верхняя тень (медвежье давление)")

    c5 = ind.stoch_k < ind.stoch_k_prev
    conds["stoch_turning_down"] = c5
    if c5:
        met += 1
        parts.append(f"Stoch поворачивает вниз ({ind.stoch_k:.0f})")

    # Confidence: conditions-driven (not hardcoded)
    base_conf = (met / _TOTAL) * 85
    if _divergence_at_level(high2, levels.resistances):
        base_conf += 7
    if rsi_diff > 10:
        base_conf += 5
    if mode == "RANGE":
        base_conf += 3

    return met, base_conf, parts, conds


def _divergence_at_level(price: float, levels: list[float]) -> bool:
    """Is divergence extreme close to a known S/R level?"""
    return any(abs(price - lvl) / max(price, 1e-10) < 0.002 for lvl in levels)


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "divergence", reason, extra or {})
