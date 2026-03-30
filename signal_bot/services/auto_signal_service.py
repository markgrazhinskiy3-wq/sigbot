"""
Auto-signal broadcaster — two-phase flow:
  Phase 1 (pre-alert): "Open [PAIR] in PO — signal coming in ~20 sec"
  Phase 2 (signal):    Actual BUY / SELL after re-checking on fresh candles
"""

from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)

# Per-user opt-in flag — in-memory cache, loaded from DB at startup
_auto_enabled: dict[int, bool] = {}

# Per-pair cooldown: don't re-send same pair within COOLDOWN_SEC
_pair_cooldown: dict[str, float] = {}
COOLDOWN_SEC = 300          # 5 minutes — minimum gap between signals on same pair

AUTO_EXPIRY_SEC       = 120  # 2-minute option
SCAN_INTERVAL_SEC     = 60   # scan all pairs every minute
MIN_CONFIDENCE        = 65   # minimum confidence to trigger pre-alert
PRE_ALERT_WAIT_MAX    = 55   # max seconds to wait for a fresh cache refresh (~45s cycle + buffer)
PRE_ALERT_POLL_SEC    = 2    # how often to poll while waiting for cache refresh


def is_auto_enabled(user_id: int) -> bool:
    """Sync check against in-memory cache (DB-backed on startup & toggle)."""
    return _auto_enabled.get(user_id, False)


async def set_auto_enabled(user_id: int, enabled: bool) -> None:
    """Toggle auto-signals and persist to DB so it survives restarts."""
    from db.database import set_auto_signals
    _auto_enabled[user_id] = enabled
    try:
        await set_auto_signals(user_id, enabled)
    except Exception as exc:
        logger.warning("Failed to persist auto_signals for %d: %s", user_id, exc)


async def auto_signal_loop(bot) -> None:
    """
    Persistent background task.
    Started once in main.py after WS auth is ready.
    """
    import config
    from services.candle_cache import get_cached as get_candles
    from services.strategy_engine import calculate_signal
    from services.signal_service import SignalResponse, format_signal_message
    from db.database import list_approved

    logger.info("Auto-signal broadcaster started (expiry=%ds, min_conf=%d, pre_alert=%ds)",
                AUTO_EXPIRY_SEC, MIN_CONFIDENCE, PRE_ALERT_DELAY)

    # Restore auto-signals preferences from DB (survives restarts)
    try:
        from db.database import load_all_auto_signals
        restored = await load_all_auto_signals()
        _auto_enabled.update(restored)
        enabled_count = sum(1 for v in restored.values() if v)
        logger.info("Auto-signals: restored %d user settings (%d enabled)",
                    len(restored), enabled_count)
    except Exception as exc:
        logger.warning("Could not restore auto-signals from DB: %s", exc)

    # Give the candle cache a chance to warm up before first scan
    await asyncio.sleep(120)

    while True:
        try:
            await _scan_and_broadcast(
                bot=bot,
                pairs=config.OTC_PAIRS,
                calculate_signal=calculate_signal,
                format_signal_message=format_signal_message,
                SignalResponse=SignalResponse,
                list_approved=list_approved,
                get_candles=get_candles,
            )
        except Exception as exc:
            logger.error("Auto-signal loop error: %s", exc, exc_info=True)

        await asyncio.sleep(SCAN_INTERVAL_SEC)


async def _scan_and_broadcast(
    bot,
    pairs: list[dict],
    calculate_signal,
    format_signal_message,
    SignalResponse,
    list_approved,
    get_candles,
) -> None:
    # Collect opted-in approved users
    approved = await list_approved()
    recipients = [u["user_id"] for u in approved if is_auto_enabled(u["user_id"])]
    if not recipients:
        return

    now = time.time()

    for pair in pairs:
        symbol     = pair["symbol"]
        pair_label = pair["label"]

        # Skip if pair is still on cooldown
        if now - _pair_cooldown.get(symbol, 0) < COOLDOWN_SEC:
            continue

        candles = get_candles(symbol)
        if not candles or len(candles) < 60:
            continue

        try:
            result = await calculate_signal(candles, expiry="2m")
        except Exception as exc:
            logger.debug("Auto-signal calc error %s: %s", symbol, exc)
            continue

        if result.direction not in ("BUY", "SELL"):
            continue
        if (result.confidence or 0) < MIN_CONFIDENCE:
            continue

        # Mark cooldown immediately — prevents duplicate pre-alerts on next scan
        _pair_cooldown[symbol] = now

        logger.info(
            "Auto-signal: %s %s conf=%.0f → pre-alert to %d users",
            pair_label, result.direction, result.confidence or 0, len(recipients),
        )

        # Launch pre-alert → wait → signal as a background task
        # (does not block the scanner from checking remaining pairs)
        asyncio.create_task(
            _fire_pre_alert_then_signal(
                bot=bot,
                recipients=list(recipients),
                pair_label=pair_label,
                symbol=symbol,
                calculate_signal=calculate_signal,
                format_signal_message=format_signal_message,
                SignalResponse=SignalResponse,
                get_candles=get_candles,
            )
        )

        # Only send one pair per scan cycle to avoid flooding
        break


async def _fire_pre_alert_then_signal(
    bot,
    recipients: list[int],
    pair_label: str,
    symbol: str,
    calculate_signal,
    format_signal_message,
    SignalResponse,
    get_candles,
) -> None:
    """
    Two-phase delivery:
      1. Pre-alert  — tells users which pair to open in PO
      2. Wait for the candle cache to actually refresh with new market data
      3. Signal     — BUY / SELL on fresh candles (or cancellation if conditions changed)
    """
    from bot.i18n import get_lang, t
    from bot.keyboards import signal_result_keyboard
    from services.candle_cache import get_cache_fetched_at

    # Snapshot the cache age BEFORE sending the pre-alert
    snapshot_fetched_at = get_cache_fetched_at(symbol)

    # ── Phase 1: pre-alert ────────────────────────────────────────────────────
    for user_id in recipients:
        try:
            lang = get_lang(user_id)
            text = t("auto_pre_alert", lang, pair=pair_label)
            await bot.send_message(user_id, text, parse_mode="HTML")
        except Exception as exc:
            logger.debug("Pre-alert send failed user %d: %s", user_id, exc)

    # ── Wait for a genuine cache refresh (new 15s candle data) ───────────────
    # The background refresher updates every ~45s. We poll every 2s until the
    # fetched_at timestamp changes — meaning real new market data has arrived.
    deadline = time.monotonic() + PRE_ALERT_WAIT_MAX
    while time.monotonic() < deadline:
        await asyncio.sleep(PRE_ALERT_POLL_SEC)
        if get_cache_fetched_at(symbol) > snapshot_fetched_at:
            logger.debug("Fresh cache received for %s — proceeding to signal", symbol)
            break
    else:
        logger.warning(
            "Cache for %s did not refresh within %ds — using best available data",
            symbol, PRE_ALERT_WAIT_MAX,
        )

    # ── Phase 2: re-check on fresh (or best available) candles ───────────────
    candles = get_candles(symbol)
    signal = None
    if candles and len(candles) >= 60:
        try:
            fresh = await calculate_signal(candles, expiry="2m")
            if fresh.direction in ("BUY", "SELL") and (fresh.confidence or 0) >= MIN_CONFIDENCE:
                signal = SignalResponse(
                    direction=fresh.direction,
                    confidence=fresh.confidence,
                    details=fresh.details,
                    pair=pair_label,
                    expiration_sec=AUTO_EXPIRY_SEC,
                    symbol=symbol,
                )
        except Exception as exc:
            logger.debug("Re-check calc failed %s: %s", symbol, exc)

    if signal is None:
        # Conditions changed — notify users so they don't wait in vain
        logger.info("Auto-signal for %s cancelled after pre-alert (conditions changed)", symbol)
        for user_id in recipients:
            try:
                lang = get_lang(user_id)
                await bot.send_message(
                    user_id,
                    t("auto_signal_cancelled", lang, pair=pair_label),
                    parse_mode="HTML",
                )
            except Exception as exc:
                logger.debug("Cancellation send failed user %d: %s", user_id, exc)
        return

    # ── Send actual signal ────────────────────────────────────────────────────
    logger.info(
        "Auto-signal fired: %s %s conf=%.0f → %d users",
        pair_label, signal.direction, signal.confidence or 0, len(recipients),
    )

    for user_id in recipients:
        try:
            lang   = get_lang(user_id)
            header = t("auto_signal_header", lang)
            body   = format_signal_message(signal, is_admin=False)
            text   = f"{header}\n\n{body}"
            await bot.send_message(
                user_id,
                text,
                parse_mode="HTML",
                reply_markup=signal_result_keyboard(symbol, AUTO_EXPIRY_SEC, lang=lang),
            )
        except Exception as exc:
            logger.debug("Signal send failed user %d: %s", user_id, exc)
