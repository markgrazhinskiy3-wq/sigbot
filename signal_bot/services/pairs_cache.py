"""
In-memory cache for available OTC pairs with payout percentages.
Refreshes automatically every CACHE_TTL_SEC seconds.
Falls back to the static config.OTC_PAIRS list if the browser fetch fails.
"""
import asyncio
import logging
import time

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger(__name__)

CACHE_TTL_SEC = 15 * 60   # 15 minutes
MIN_PAYOUT    = 82        # only show pairs with payout >= this (recommended pairs threshold)

_cache: list[dict] = []
_cache_ts: float   = 0.0
_lock = asyncio.Lock()


def _fallback_pairs() -> list[dict]:
    """Return static config pairs — used when browser fetch fails."""
    return [
        {"label": p["label"], "symbol": p["symbol"], "payout": 0, "name": p["label"]}
        for p in config.OTC_PAIRS
    ]


def is_fresh() -> bool:
    return bool(_cache) and (time.time() - _cache_ts) < CACHE_TTL_SEC


def get_cached() -> list[dict]:
    """Return cached pairs or fallback (never empty so keyboard always renders)."""
    return _cache if _cache else _fallback_pairs()


async def refresh(force: bool = False) -> list[dict]:
    """
    Fetch live OTC pairs from PocketOption.
    Thread-safe — concurrent callers wait on the lock and reuse the result.
    Returns the updated (or cached) pairs list.
    """
    global _cache, _cache_ts

    async with _lock:
        if not force and is_fresh():
            return _cache

        logger.info("Refreshing OTC pairs cache (force=%s)…", force)
        try:
            from services.pocket_browser import get_available_otc_pairs
            pairs = await get_available_otc_pairs(min_payout=MIN_PAYOUT)
        except Exception as e:
            logger.warning("OTC pairs fetch failed, using fallback: %s", e)
            pairs = []

        if pairs:
            _cache    = pairs
            _cache_ts = time.time()
            logger.info("Pairs cache updated: %d pairs", len(pairs))
        else:
            logger.warning("No pairs returned — keeping previous cache or fallback")
            if not _cache:
                _cache = _fallback_pairs()

        return _cache
