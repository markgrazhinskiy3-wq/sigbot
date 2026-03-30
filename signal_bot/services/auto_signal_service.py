"""
Auto-signal broadcaster.
Background loop that scans all OTC_PAIRS every 60 seconds and sends
signals to users who opted in. Uses 5-minute expiry.
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
COOLDOWN_SEC = 300          # 5 minutes

AUTO_EXPIRY_SEC   = 120     # 2-minute option — gives users time to navigate to the pair in PO
SCAN_INTERVAL_SEC = 60      # check every minute
MIN_CONFIDENCE    = 65      # only broadcast high-quality signals


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

    logger.info("Auto-signal broadcaster started (expiry=%ds, min_conf=%d)",
                AUTO_EXPIRY_SEC, MIN_CONFIDENCE)

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
    from bot.i18n import get_lang, t
    from bot.keyboards import signal_result_keyboard

    # Collect opted-in approved users
    approved = await list_approved()
    recipients = [u["user_id"] for u in approved if is_auto_enabled(u["user_id"])]
    if not recipients:
        return

    now = time.time()

    for pair in pairs:
        symbol    = pair["symbol"]
        pair_label = pair["label"]

        # Skip if pair is still on cooldown
        if now - _pair_cooldown.get(symbol, 0) < COOLDOWN_SEC:
            continue

        candles = get_candles(symbol)
        if not candles or len(candles) < 60:
            continue

        try:
            result = await calculate_signal(candles, expiry="1m")
        except Exception as exc:
            logger.debug("Auto-signal calc error %s: %s", symbol, exc)
            continue

        if result.direction not in ("BUY", "SELL"):
            continue
        if (result.confidence or 0) < MIN_CONFIDENCE:
            continue

        # Good signal found — mark cooldown immediately to avoid duplicates
        _pair_cooldown[symbol] = now

        signal = SignalResponse(
            direction=result.direction,
            confidence=result.confidence,
            details=result.details,
            pair=pair_label,
            expiration_sec=AUTO_EXPIRY_SEC,
            symbol=symbol,
        )

        logger.info(
            "Auto-signal: %s %s conf=%.0f → broadcasting to %d users",
            pair_label, result.direction, result.confidence or 0, len(recipients),
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
                logger.debug("Auto-signal send failed user %d: %s", user_id, exc)
