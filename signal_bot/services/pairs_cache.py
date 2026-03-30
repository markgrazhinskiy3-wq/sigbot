"""
In-memory cache for available OTC pairs.
Pairs are filtered to >= MIN_PAYOUT (80%) when live payout data is available.
Sorted alphabetically to match PocketOption UI order.
Payout % is stored in each pair dict and shown in button labels.
"""
import asyncio
import logging
import time

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger(__name__)

CACHE_TTL_SEC = 5 * 60   # refresh every 5 minutes to pick up live payouts
MIN_PAYOUT    = 80        # filter threshold (used only when live data is available)

_cache: list[dict] = []
_cache_ts: float   = 0.0
_lock = asyncio.Lock()


def _all_config_pairs() -> list[dict]:
    """All pairs from config — payout 0 means unknown (no live data yet)."""
    result = []
    for p in config.OTC_PAIRS:
        name = p["label"]
        result.append({
            "label":  name,
            "symbol": p["symbol"],
            "payout": 0,   # unknown until live WS data arrives
            "name":   name,
        })
    return result


def _dynamic_pairs(live_assets: dict) -> list[dict] | None:
    """
    Build the full pair list from live PocketOption asset registry.
    Only includes pairs that are in config.OTC_PAIRS (our supported set).
    Live data is used to pull the current payout % for each pair.
    Returns None if no suitable pairs found (caller falls back to config).
    """
    if not live_assets:
        return None

    # Build lookup: normalised symbol → config entry
    _supported: dict[str, dict] = {
        p["symbol"].lower(): p for p in config.OTC_PAIRS
    }

    result = []
    for key, asset in live_assets.items():
        if asset.get("category") != "currency":
            continue
        if "_otc" not in key:
            continue
        sym = asset["symbol"]        # e.g. "AEDCNY_otc"  (no leading #)
        full_sym = f"#{sym}"         # e.g. "#AEDCNY_otc"
        # Only include pairs we have optimised strategies for
        if full_sym.lower() not in _supported:
            continue
        payout = asset["payout"]
        if payout < MIN_PAYOUT:
            continue
        name = asset["name"]         # e.g. "AED/CNY OTC"
        result.append({
            "label":  name,
            "symbol": full_sym,
            "payout": payout,
            "name":   name,
        })

    if not result:
        logger.warning(
            "live_assets present (%d entries) but no currency OTC >= %d%%",
            len(live_assets), MIN_PAYOUT,
        )
        return None

    # Sort: payout desc, then alphabetically — matches PocketOption "Выплата ▼"
    result.sort(key=lambda x: (-x["payout"], x["label"]))
    return result


def _filtered_pairs(live_payouts: dict) -> list[dict] | None:
    """
    Fallback: build filtered list from config pairs using live payout data.
    Used only when live_assets is unavailable.
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
        return None

    result.sort(key=lambda x: (-x["payout"], x["label"]))
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

        # Source 1: full asset registry from updateAssets WS event (most complete)
        try:
            from services.po_ws_client import get_live_assets
            assets = get_live_assets()
            if assets:
                pairs = _dynamic_pairs(assets)
                if pairs:
                    logger.info(
                        "Pairs from live assets (>= %d%%): %d currency OTC pairs",
                        MIN_PAYOUT, len(pairs),
                    )
        except Exception as e:
            logger.warning("Could not read live assets: %s", e)

        # Source 2: fallback — config pairs filtered by WS payout data
        if not pairs:
            try:
                from services.po_ws_client import get_live_payouts
                wp = get_live_payouts()
                if wp:
                    pairs = _filtered_pairs(wp)
                    if pairs:
                        logger.info(
                            "Pairs from live payouts fallback (>= %d%%): %d pairs",
                            MIN_PAYOUT, len(pairs),
                        )
            except Exception as e:
                logger.warning("Could not read live payouts: %s", e)

        # No live data — show all config pairs
        if not pairs:
            pairs = _all_config_pairs()
            logger.info("No live asset data yet — showing all %d config pairs", len(pairs))

        _cache    = pairs
        _cache_ts = time.time()
        return _cache
