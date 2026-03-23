"""
In-memory cache for available OTC pairs.
Returns pairs from config instantly — no browser scraping, no payout display.
"""
import asyncio
import logging
import time

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger(__name__)

CACHE_TTL_SEC = 60 * 60   # 1 hour (irrelevant since we use static config)
MIN_PAYOUT    = 0          # no payout filtering — show all config pairs

_cache: list[dict] = []
_cache_ts: float   = 0.0
_lock = asyncio.Lock()


def _build_pairs() -> list[dict]:
    """Build pairs list from config — no browser needed, instant."""
    result = []
    for p in config.OTC_PAIRS:
        payout = p.get("payout", 82)
        name   = p["label"]
        result.append({
            "label":  name,   # clean label, no % shown
            "symbol": p["symbol"],
            "payout": payout,
            "name":   name,
        })
    return result


def is_fresh() -> bool:
    return bool(_cache) and (time.time() - _cache_ts) < CACHE_TTL_SEC


def get_cached() -> list[dict]:
    """Return cached pairs — always non-empty."""
    if not _cache:
        return _build_pairs()
    return _cache


async def refresh(force: bool = False) -> list[dict]:
    """Return OTC pairs from config instantly."""
    global _cache, _cache_ts

    async with _lock:
        if not force and is_fresh():
            return _cache

        pairs = _build_pairs()
        _cache    = pairs
        _cache_ts = time.time()
        logger.info("Pairs cache loaded: %d pairs from config", len(pairs))
        return _cache
