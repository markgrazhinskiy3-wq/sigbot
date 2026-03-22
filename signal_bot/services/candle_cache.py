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

# How many candles to keep per pair (enough for all analysis modules)
CANDLE_COUNT: int = 80


class _CacheEntry(NamedTuple):
    candles: list[dict]
    fetched_at: float   # unix timestamp


_cache: dict[str, _CacheEntry] = {}
_refresher_task: asyncio.Task | None = None
_warm_up_done: bool = False


def is_warm_up_done() -> bool:
    """Returns True once the initial browser warm-up cycle has completed."""
    return _warm_up_done


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
        logger.debug("Cache hit: %s (age=%.1fs, %d candles)", symbol, age, len(entry.candles))
        return list(entry.candles)   # return a copy so callers can't mutate cache
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
    new_with_time  = [c for c in new_candles  if c.get("time", 0) > 0]
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
        by_time[c["time"]] = c   # new data takes precedence

    merged = sorted(by_time.values(), key=lambda c: c["time"])
    trimmed = merged[-CANDLE_COUNT:] if len(merged) > CANDLE_COUNT else merged
    _cache[symbol] = _CacheEntry(candles=trimmed, fetched_at=time.time())
    logger.debug(
        "Cache merged: %s (%d existing + %d new = %d total)",
        symbol, len(existing.candles), len(new_candles), len(trimmed),
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
        len(pairs), CACHE_TTL, REFRESH_INTERVAL,
    )

    # Initial warm-up:
    # 1. Load ALL pairs via browser to build full history (50+ candles each)
    #    — this provides the baseline data for quality signal analysis.
    # 2. Simultaneously, first browser load captures WS auth for future cycles.
    # WS is NOT used for initial warm-up (it only gives 13-14 candles).
    #
    # _warm_up_done is set True after MIN_READY_PAIRS are loaded so users
    # aren't blocked while the remaining pairs continue loading in background.
    MIN_READY_PAIRS = min(10, len(pairs))
    logger.info("Initial cache warm-up (browser, %d pairs, ready after %d)...", len(pairs), MIN_READY_PAIRS)
    loaded = 0
    global _warm_up_done
    for symbol in pairs:
        ok = await _refresh_one_browser(symbol)
        if ok:
            loaded += 1
            entry = _cache.get(symbol)
            logger.info(
                "Warm-up: %s cached (%d candles) [%d/%d]",
                symbol,
                len(entry.candles) if entry else 0,
                loaded, len(pairs),
            )
            if not _warm_up_done and loaded >= MIN_READY_PAIRS:
                _warm_up_done = True
                logger.info("Cache warm-up threshold reached (%d pairs). Bot is now ready.", loaded)
    if not _warm_up_done:
        _warm_up_done = True
    logger.info("Cache warm-up complete (%d/%d pairs). WS auth available: %s", loaded, len(pairs), ws_available())

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
                ok, len(pairs), elapsed,
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
                        "Signal refresh via WS: %s (%d candles)", symbol, len(entry.candles)
                    )
                    return list(entry.candles)
        except Exception as e:
            logger.warning("WS signal-refresh failed for %s: %s — using cache", symbol, e)

    # Fallback: use whatever is in the cache
    cached = get_cached(symbol)
    if cached:
        return cached
    return []


def resample_to_1m(candles_15s: list[dict]) -> list[dict]:
    """
    Resample 15-second candles into 1-minute candles.
    Groups by 1-minute bucket (time // 60 * 60).
    Returns list sorted oldest → newest.
    Typically produces ~10-14 bars from 50 fifteen-second bars.
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
        result.append({
            "time":  bucket,
            "open":  g[0]["open"],
            "high":  max(c["high"] for c in g),
            "low":   min(c["low"]  for c in g),
            "close": g[-1]["close"],
        })
    return result


def resample_to_5m(candles_1m: list[dict]) -> list[dict]:
    """
    Resample 1-minute candles into 5-minute candles.
    Groups by 5-minute bucket (time // 300 * 300).
    Returns list sorted oldest → newest.
    """
    from collections import defaultdict
    groups: dict[int, list[dict]] = defaultdict(list)
    for c in candles_1m:
        t = c.get("time", 0)
        if t <= 0:
            continue
        bucket = (t // 300) * 300
        groups[bucket].append(c)

    result = []
    for bucket in sorted(groups.keys()):
        g = groups[bucket]
        result.append({
            "time":  bucket,
            "open":  g[0]["open"],
            "high":  max(c["high"] for c in g),
            "low":   min(c["low"]  for c in g),
            "close": g[-1]["close"],
        })
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
