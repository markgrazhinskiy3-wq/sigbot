"""
Auto-signal broadcaster — two-phase flow:
  Phase 1 (pre-alert): "Open [PAIR] in PO — signal coming in ~20 sec"
  Phase 2 (signal):    Actual BUY / SELL after re-checking on fresh candles

All outgoing signals pass through SignalFilter (15 layers) before Phase 1.
Only three approved strategies are sent: three_candle_reversal, stoch_snap,
otc_trend_confirm. Signals from ema_micro_cross / rsi_bb_scalp / double_bottom_top
are still calculated for statistical purposes but silently dropped.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)

# ── Module-level singletons — created once, live for the process lifetime ────
from services.signal_filter    import SignalFilter
from services.signal_formatter import SignalFormatter
from services.trade_logger     import TradeLogger

_signal_filter    = SignalFilter()
_signal_formatter = SignalFormatter()

# CSV logs land next to signal_bot.db (configurable via env var)
_log_dir     = os.path.dirname(os.path.abspath(__file__ + "/../signal_bot.db"))
_trade_logger = TradeLogger(
    trades_path   = os.path.join(_log_dir, "trades_log.csv"),
    rejected_path = os.path.join(_log_dir, "rejected_log.csv"),
)

# Per-user opt-in flag — in-memory cache, loaded from DB at startup
_auto_enabled: dict[int, bool] = {}

# Per-pair cooldown: don't re-send same pair within COOLDOWN_SEC
_pair_cooldown: dict[str, float] = {}
COOLDOWN_SEC          = 300   # 5 minutes — minimum gap between signals on same pair

AUTO_EXPIRY_SEC       = 120   # 2-minute option
SCAN_INTERVAL_SEC     = 60    # scan all pairs every minute
MIN_CONFIDENCE        = 55    # raw engine confidence floor (filter adds its own checks)
PRE_ALERT_WAIT_MAX    = 55    # max seconds to wait for a fresh cache refresh
PRE_ALERT_POLL_SEC    = 2     # how often to poll while waiting


# ── Public helpers ────────────────────────────────────────────────────────────

def is_auto_enabled(user_id: int) -> bool:
    return _auto_enabled.get(user_id, False)


async def set_auto_enabled(user_id: int, enabled: bool) -> None:
    from db.database import set_auto_signals
    _auto_enabled[user_id] = enabled
    try:
        await set_auto_signals(user_id, enabled)
    except Exception as exc:
        logger.warning("Failed to persist auto_signals for %d: %s", user_id, exc)


def notify_outcome(pair: str, result: str, pnl: float | None = None,
                   close_price: float | None = None) -> None:
    """
    Called by outcome_tracker after a trade closes.
    Updates the SignalFilter streak counters and TradeLogger CSV.
    """
    _signal_filter.update_result(pair, result)
    _trade_logger.update_trade_result(pair, result, pnl, close_price)
    logger.info("Auto-signal outcome registered: %s → %s", pair, result)


def get_filter_stats() -> dict:
    """Return current filter state (active trades, loss streak, etc.)."""
    return _signal_filter.get_stats()


# ── Main loop ─────────────────────────────────────────────────────────────────

async def auto_signal_loop(bot) -> None:
    """
    Persistent background task.
    Started once in main.py after WS auth is ready.
    """
    import config
    from services.candle_cache    import get_cached as get_candles
    from services.strategy_engine import calculate_signal
    from services.signal_service  import SignalResponse, format_signal_message
    from db.database              import list_approved

    logger.info(
        "Auto-signal broadcaster started (expiry=%ds, min_conf=%d, pre_alert_max=%ds)",
        AUTO_EXPIRY_SEC, MIN_CONFIDENCE, PRE_ALERT_WAIT_MAX,
    )

    # Restore auto-signals preferences from DB
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

    # Start the live WR refresh task (runs every 10 minutes in background)
    asyncio.create_task(_live_wr_refresh_loop())

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


# ── Scan cycle ────────────────────────────────────────────────────────────────

async def _scan_and_broadcast(
    bot,
    pairs: list[dict],
    calculate_signal,
    format_signal_message,
    SignalResponse,
    list_approved,
    get_candles,
) -> None:
    approved_users = await list_approved()
    recipients = [u["user_id"] for u in approved_users if is_auto_enabled(u["user_id"])]
    if not recipients:
        return

    now = time.time()

    for pair in pairs:
        symbol     = pair["symbol"]
        pair_label = pair["label"]

        # Per-pair cooldown (5 min — prevents duplicate blasts)
        if now - _pair_cooldown.get(symbol, 0) < COOLDOWN_SEC:
            continue

        candles = get_candles(symbol)
        if not candles or len(candles) < 60:
            continue

        try:
            result = await calculate_signal(candles, expiry="both", symbol=symbol)
        except Exception as exc:
            logger.debug("Auto-signal calc error %s: %s", symbol, exc)
            continue

        if result.direction not in ("BUY", "SELL"):
            continue

        # Quick raw-confidence pre-check before building full filter dict
        conf_raw = result.details.get("confidence_raw", 0)
        if conf_raw < MIN_CONFIDENCE:
            continue

        # ── Build signal dict for SignalFilter ────────────────────────────────
        session  = _derive_session(result.details)
        strategy = result.details.get("primary_strategy", "")
        expiry   = result.details.get("expiry_hint", "1m")
        entry_price = result.details.get("debug", {}).get("last_close", 0)

        signal_dict = {
            "pair":        pair_label,
            "direction":   result.direction,
            "strategy":    strategy,
            "expiry":      expiry,
            "confidence":  conf_raw,
            "session":     session,
            "entry_price": entry_price,
            "entry_time":  now,
        }

        # ── Run through all 15 filters ────────────────────────────────────────
        filter_result = _signal_filter.check(signal_dict)

        if not filter_result["approved"]:
            # Log rejection silently (no user message — just CSV + debug log)
            _trade_logger.log_rejected(filter_result)
            logger.debug(
                "Auto-signal REJECTED %s %s: %s",
                pair_label, result.direction, filter_result["reason"],
            )
            continue

        # ── Filter passed — log the approved trade ────────────────────────────
        _trade_logger.log_trade(filter_result)

        # Lock cooldown before pre-alert (prevents duplicates on next scan)
        _pair_cooldown[symbol] = now

        new_conf = filter_result["new_confidence"]
        logger.info(
            "Auto-signal: %s %s strategy=%s conf_raw=%.0f → new_conf=%.0f "
            "session=%s → pre-alert to %d users",
            pair_label, result.direction, strategy, conf_raw, new_conf,
            session, len(recipients),
        )

        # Launch pre-alert → wait → signal as background task
        asyncio.create_task(
            _fire_pre_alert_then_signal(
                bot=bot,
                recipients=list(recipients),
                pair_label=pair_label,
                symbol=symbol,
                filter_result=filter_result,
                calculate_signal=calculate_signal,
                format_signal_message=format_signal_message,
                SignalResponse=SignalResponse,
                get_candles=get_candles,
            )
        )

        # Only one signal per scan cycle to avoid flooding
        break


# ── Two-phase delivery ────────────────────────────────────────────────────────

async def _fire_pre_alert_then_signal(
    bot,
    recipients: list[int],
    pair_label: str,
    symbol: str,
    filter_result: dict,
    calculate_signal,
    format_signal_message,
    SignalResponse,
    get_candles,
) -> None:
    """
    Phase 1: Pre-alert — tells users which pair to open in PO.
    Phase 2: Re-check on fresh candles, run through filter again.
    Phase 3: Send the confirmed signal with recalculated confidence.
    """
    from bot.i18n         import get_lang, t
    from bot.keyboards    import signal_result_keyboard
    from services.candle_cache import get_cache_fetched_at

    approved_sig  = filter_result["signal"]
    new_conf      = filter_result["new_confidence"]
    n_passed      = len(filter_result["filters_passed"])

    snapshot_fetched_at = get_cache_fetched_at(symbol)

    # ── Phase 1: pre-alert ────────────────────────────────────────────────────
    for user_id in recipients:
        try:
            lang = get_lang(user_id)
            text = t("auto_pre_alert", lang, pair=pair_label)
            await bot.send_message(user_id, text, parse_mode="HTML")
        except Exception as exc:
            logger.debug("Pre-alert send failed user %d: %s", user_id, exc)

    # ── Phase 2: wait for genuine cache refresh ───────────────────────────────
    deadline = time.monotonic() + PRE_ALERT_WAIT_MAX
    while time.monotonic() < deadline:
        await asyncio.sleep(PRE_ALERT_POLL_SEC)
        if get_cache_fetched_at(symbol) > snapshot_fetched_at:
            logger.debug("Fresh cache received for %s — proceeding to signal", symbol)
            break
    else:
        logger.warning("Cache for %s did not refresh within %ds — using best available data",
                       symbol, PRE_ALERT_WAIT_MAX)

    # ── Phase 3: re-check on fresh candles ───────────────────────────────────
    candles = get_candles(symbol)
    signal  = None

    if candles and len(candles) >= 60:
        try:
            fresh = await calculate_signal(candles, expiry="both", symbol=symbol)

            if fresh.direction in ("BUY", "SELL"):
                fresh_conf_raw = fresh.details.get("confidence_raw", 0)
                fresh_strategy = fresh.details.get("primary_strategy", "")

                # Direction and strategy must still match the pre-alerted signal
                if (fresh.direction   == approved_sig["direction"] and
                        fresh_strategy == approved_sig["strategy"] and
                        fresh_conf_raw >= MIN_CONFIDENCE):

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

    # ── Send the final signal ─────────────────────────────────────────────────
    from db.database              import save_signal_outcome
    from services.outcome_tracker import track_outcome
    from services import analytics_logger

    strategy    = approved_sig.get("strategy", "")
    signal_price = float(
        signal.details.get("debug", {}).get("last_close", 0)
        or approved_sig.get("entry_price", 0)
    )
    market_mode  = signal.details.get("market_mode", "")
    conf_raw_out = signal.details.get("confidence_raw", new_conf)
    conf_band    = signal.details.get("signal_quality", "")

    logger.info(
        "Auto-signal fired: %s %s strategy=%s new_conf=%.0f price=%.6f → %d users",
        pair_label, signal.direction, strategy, new_conf, signal_price, len(recipients),
    )

    for user_id in recipients:
        try:
            lang = get_lang(user_id)

            # Use SignalFormatter for the custom filtered-signal body
            signal_body = _signal_formatter.format_signal(filter_result)
            header      = t("auto_signal_header", lang)
            text        = f"{header}\n\n{signal_body}"

            await bot.send_message(
                user_id,
                text,
                parse_mode="HTML",
                reply_markup=signal_result_keyboard(
                    symbol, AUTO_EXPIRY_SEC, lang=lang
                ),
            )

            # ── Start outcome tracking for this user ──────────────────────────
            # Save a pending record in DB → get outcome_id → wait expiry → notify
            if signal_price > 0:
                try:
                    outcome_id = await save_signal_outcome(
                        user_id        = user_id,
                        symbol         = symbol,
                        pair_label     = pair_label,
                        direction      = signal.direction,
                        confidence     = signal.confidence,   # stars (0-5)
                        strategy       = strategy,
                        expiration_sec = AUTO_EXPIRY_SEC,
                        signal_price   = signal_price,
                        market_mode    = market_mode,
                        confidence_raw = conf_raw_out,
                        confidence_band = conf_band,
                    )
                    asyncio.create_task(track_outcome(
                        bot            = bot,
                        chat_id        = user_id,
                        outcome_id     = outcome_id,
                        symbol         = symbol,
                        pair_label     = pair_label,
                        direction      = signal.direction,
                        strategy       = strategy,
                        expiration_sec = AUTO_EXPIRY_SEC,
                        signal_price   = signal_price,
                    ))
                    asyncio.create_task(analytics_logger.log_signal(
                        outcome_id  = outcome_id,
                        pair        = pair_label,
                        symbol      = symbol,
                        direction   = signal.direction,
                        expiry      = "2m" if AUTO_EXPIRY_SEC >= 120 else "1m",
                        entry_price = signal_price,
                        details     = signal.details,
                    ))
                except Exception as track_exc:
                    logger.warning("Outcome tracking setup failed for user %d: %s",
                                   user_id, track_exc)

        except Exception as exc:
            logger.debug("Signal send failed user %d: %s", user_id, exc)


# ── Live WR refresh ───────────────────────────────────────────────────────────

_LIVE_WR_REFRESH_SEC = 600  # 10 minutes


async def _live_wr_refresh_loop() -> None:
    """
    Background task: every 10 minutes, refresh two WR caches in SignalFilter:
      1. Strategy-level WR  (last 40 trades, min 10) — broad fallback
      2. Pair×strategy WR   (all history, min 5)     — most specific, highest priority

    Together these implement a three-level hierarchy in _recalculate_confidence:
      pair×strategy → strategy → static config
    """
    from db.database import get_all_strategies_live_wr, get_all_pair_strategy_live_wr

    _all_tracked_strategies = list(
        _signal_filter.config.get("strategy_wr", {}).keys()
    )

    logger.info("Live WR refresh task started (interval=%ds)", _LIVE_WR_REFRESH_SEC)

    while True:
        await asyncio.sleep(_LIVE_WR_REFRESH_SEC)
        try:
            # Level 2: strategy-level WR
            live_wr = await get_all_strategies_live_wr(
                strategies=_all_tracked_strategies,
                n_trades=40,
                min_trades=10,
            )
            _signal_filter.update_live_wr(live_wr)
        except Exception as exc:
            logger.warning("Live WR (strategy) refresh failed: %s", exc)

        try:
            # Level 1: pair×strategy WR — most specific
            pair_wr = await get_all_pair_strategy_live_wr(min_trades=5)
            _signal_filter.update_live_pair_wr(pair_wr)
        except Exception as exc:
            logger.warning("Live WR (pair×strategy) refresh failed: %s", exc)


# ── Session detection ─────────────────────────────────────────────────────────

def _derive_session(details: dict) -> str:
    """
    Derive market session (BULL/BEAR/NEUTRAL) from engine debug flags.

    BULL:    1m EMA or macro slope is upward
    BEAR:    1m EMA or macro slope is downward
    NEUTRAL: no clear directional bias in either timeframe
    """
    dbg          = details.get("debug", {})
    ctx_up_1m    = bool(dbg.get("ctx_up_1m",    False))
    ctx_dn_1m    = bool(dbg.get("ctx_dn_1m",    False))
    ctx_macro_up = bool(dbg.get("ctx_macro_up", False))
    ctx_macro_dn = bool(dbg.get("ctx_macro_dn", False))

    is_bull = ctx_up_1m or ctx_macro_up
    is_bear = ctx_dn_1m or ctx_macro_dn

    if is_bull and not is_bear:
        return "BULL"
    if is_bear and not is_bull:
        return "BEAR"
    if is_bull and is_bear:
        # Mixed: pick dominant context (1m takes priority over macro)
        return "BULL" if ctx_up_1m else "BEAR"
    return "NEUTRAL"
