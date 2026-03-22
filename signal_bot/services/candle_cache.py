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
CACHE_TTL: int = 55

# How often the background loop refreshes all pairs (should be < CACHE_TTL)
REFRESH_INTERVAL: int = 45

# How many candles to keep per pair (enough for all analysis modules)
CANDLE_COUNT: int = 80


class _CacheEntry(NamedTuple):
    candles: list[dict]
    fetched_at: float   # unix timestamp


_cache: dict[str, _CacheEntry] = {}
_refresher_task: asyncio.Task | None = None


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
    """Store freshly-fetched candles in the cache."""
    _cache[symbol] = _CacheEntry(candles=list(candles), fetched_at=time.time())
    logger.debug("Cache stored: %s (%d candles)", symbol, len(candles))


async def _refresh_one(symbol: str) -> bool:
    """Fetch candles for one symbol and update the cache. Returns True on success."""
    from services.pocket_browser import _get_candles_impl, _get_lock
    try:
        async with asyncio.timeout(90):
            async with _get_lock():
                candles = await _get_candles_impl(symbol, CANDLE_COUNT)
        if candles:
            store(symbol, candles)
            return True
        logger.warning("Cache refresh returned empty candles for %s", symbol)
        return False
    except TimeoutError:
        logger.error("Cache refresh timed out for %s", symbol)
        return False
    except Exception as e:
        logger.error("Cache refresh failed for %s: %s", symbol, e)
        return False


async def _refresher_loop(pairs: list[str]) -> None:
    """
    Continuously refresh all pairs in rotation.
    Sleeps REFRESH_INTERVAL seconds between full cycles.
    """
    logger.info(
        "Candle cache refresher started — %d pairs, TTL=%ds, interval=%ds",
        len(pairs), CACHE_TTL, REFRESH_INTERVAL,
    )

    # Stagger initial load: refresh all pairs once before entering the cycle
    logger.info("Initial cache warm-up: loading %d pairs...", len(pairs))
    for symbol in pairs:
        await _refresh_one(symbol)
    logger.info("Cache warm-up complete.")

    while True:
        await asyncio.sleep(REFRESH_INTERVAL)
        cycle_start = time.time()
        ok = 0
        for symbol in pairs:
            if await _refresh_one(symbol):
                ok += 1
        elapsed = round(time.time() - cycle_start, 1)
        logger.info(
            "Cache refresh cycle: %d/%d pairs updated in %.1fs",
            ok, len(pairs), elapsed,
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
