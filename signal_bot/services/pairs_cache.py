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
MIN_PAYOUT    = 80        # only show pairs with payout >= this (recommended pairs threshold)

_cache: list[dict] = []
_cache_ts: float   = 0.0
_lock = asyncio.Lock()


def _fallback_pairs() -> list[dict]:
    """Return static config pairs — used when browser fetch fails.
    Uses payout from config if present, so the UI always shows a %.
    """
    result = []
    for p in config.OTC_PAIRS:
        payout = p.get("payout", 0)
        label  = p["label"]
        if payout > 0 and "%" not in label:
            label = f"{label} | {payout}%"
        result.append({"label": label, "symbol": p["symbol"], "payout": payout, "name": p["label"]})
    return result


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

    Strategy:
      1. Browser scraping (Strategy 0/1/2/3 inside pocket_browser)
      2. Direct WS payout fetch — connect via the same auth credentials the
         candle fetcher uses, drain events and parse payout fields
      3. Static fallback from config (shows estimate payouts)
    """
    global _cache, _cache_ts

    async with _lock:
        if not force and is_fresh():
            return _cache

        logger.info("Refreshing OTC pairs cache (force=%s)…", force)

        # ── Strategy 1: browser scraping ─────────────────────────────────────
        pairs: list[dict] = []
        try:
            from services.pocket_browser import get_available_otc_pairs
            pairs = await get_available_otc_pairs(min_payout=MIN_PAYOUT)
        except Exception as e:
            logger.warning("OTC pairs browser fetch failed: %s", e)

        # ── Strategy 2: direct WS payout fetch ───────────────────────────────
        if not pairs:
            logger.info("Browser scraping returned nothing — trying direct WS payout fetch…")
            try:
                from services.po_ws_client import fetch_asset_payouts
                payout_map = await fetch_asset_payouts(timeout=12.0)
                if payout_map:
                    # Build pairs list: use config symbols, substitute live payouts
                    ws_pairs: list[dict] = []
                    for p in config.OTC_PAIRS:
                        sym_key = p["symbol"].lstrip("#").lower()
                        payout = payout_map.get(sym_key, p.get("payout", 0))
                        if payout >= MIN_PAYOUT:
                            name   = p["label"]
                            label  = f"{name} | {payout}%"
                            ws_pairs.append({
                                "label":  label,
                                "symbol": p["symbol"],
                                "payout": payout,
                                "name":   name,
                            })
                    ws_pairs.sort(key=lambda x: -x["payout"])
                    if ws_pairs:
                        logger.info("WS payout fetch yielded %d pairs", len(ws_pairs))
                        pairs = ws_pairs
                    else:
                        logger.warning(
                            "WS payout fetch ran but no pairs passed min_payout=%d. "
                            "payout_map keys: %s", MIN_PAYOUT, list(payout_map)[:10],
                        )
                else:
                    logger.warning("WS payout fetch returned empty dict")
            except Exception as e:
                logger.warning("WS payout fetch failed: %s", e)

        # ── Update cache or keep previous / fallback ──────────────────────────
        if pairs:
            _cache    = pairs
            _cache_ts = time.time()
            logger.info("Pairs cache updated: %d pairs", len(pairs))
        else:
            logger.warning("All payout strategies failed — keeping previous cache or config fallback")
            if not _cache:
                _cache = _fallback_pairs()

        return _cache
