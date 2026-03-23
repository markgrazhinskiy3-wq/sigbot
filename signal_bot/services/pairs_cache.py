"""
In-memory cache for available OTC pairs.
Pairs are loaded from config instantly. Payout filtering (>= MIN_PAYOUT) is
applied if live payout data is available from the WS candle client.
Percentages are NOT shown in labels — only used for filtering.
"""
import asyncio
import logging
import time

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger(__name__)

CACHE_TTL_SEC = 5 * 60   # refresh every 5 minutes to pick up live payouts
MIN_PAYOUT    = 85        # filter threshold (used only when live data is available)

_cache: list[dict] = []
_cache_ts: float   = 0.0
_lock = asyncio.Lock()


def _all_config_pairs() -> list[dict]:
    """All pairs from config with clean labels (no %)."""
    result = []
    for p in config.OTC_PAIRS:
        name = p["label"]
        result.append({
            "label":  name,
            "symbol": p["symbol"],
            "payout": p.get("payout", 82),
            "name":   name,
        })
    return result


def _filtered_pairs(live_payouts: dict) -> list[dict] | None:
    """
    Build filtered list using live payout data.
    Returns None if live_payouts is empty (caller should use all pairs instead).
    Labels are clean — no % shown.
    """
    if not live_payouts:
        return None

    result = []
    for p in config.OTC_PAIRS:
        sym_key = p["symbol"].lstrip("#").lower()
        payout = (
            live_payouts.get(sym_key)
            or live_payouts.get(sym_key.replace("_otc", ""))
            or p.get("payout", 0)
        )
        if payout >= MIN_PAYOUT:
            name = p["label"]
            result.append({
                "label":  name,
                "symbol": p["symbol"],
                "payout": payout,
                "name":   name,
            })

    if not result:
        logger.warning(
            "Live payouts present (%d entries) but none >= %d%% — showing all pairs",
            len(live_payouts), MIN_PAYOUT,
        )
        return None

    result.sort(key=lambda x: -x["payout"])
    return result


def is_fresh() -> bool:
    return bool(_cache) and (time.time() - _cache_ts) < CACHE_TTL_SEC


def get_cached() -> list[dict]:
    """Return cached pairs — always non-empty."""
    return _cache if _cache else _all_config_pairs()


async def refresh(force: bool = False) -> list[dict]:
    """
    Refresh pairs cache.
    - If live WS payout data is available: filter by MIN_PAYOUT (85%).
    - Otherwise: return all config pairs (no filtering).
    Payout % is never shown in labels.
    """
    global _cache, _cache_ts

    async with _lock:
        if not force and is_fresh():
            return _cache

        pairs: list[dict] | None = None

        # Try live payouts captured from WS candle frames (side-effect of candle refresh)
        try:
            from services.po_ws_client import get_live_payouts
            live = get_live_payouts()
            if live:
                pairs = _filtered_pairs(live)
                if pairs:
                    logger.info(
                        "Pairs filtered by live WS payouts (>= %d%%): %d/%d pairs",
                        MIN_PAYOUT, len(pairs), len(config.OTC_PAIRS),
                    )
        except Exception as e:
            logger.warning("Could not read live WS payouts: %s", e)

        # No live data — show all config pairs
        if not pairs:
            pairs = _all_config_pairs()
            logger.info("No live payout data — showing all %d config pairs", len(pairs))

        _cache    = pairs
        _cache_ts = time.time()
        return _cache
