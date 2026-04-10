"""
Outcome Tracker
Waits for the expiration of a signal, fetches ONE candle (2-3 sec browser),
determines WIN/LOSS by comparing close price to signal price,
updates the DB, and notifies the user.

Deliberately lightweight: no demo trades, no screenshots — the browser is
free for other users' signal requests the whole time.
"""
import asyncio
import logging

from aiogram import Bot

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from db.database import resolve_outcome
from bot.keyboards import after_result_keyboard
from bot.i18n import t, get_lang
from services import analytics_logger

logger = logging.getLogger(__name__)

# ── Pip/point helpers ──────────────────────────────────────────────────────────

def _pip_multiplier(price: float) -> int:
    """
    Return multiplier to convert raw price difference to integer 'points'.
    JPY-style pairs (price > 20): 1 point = 0.001 → ×1 000
    All other forex OTC pairs:    1 point = 0.00001 → ×100 000
    """
    return 1_000 if price > 20 else 100_000


def _format_points(entry: float, close: float, direction: str, lang: str = "ru") -> str:
    """Return a signed points string relative to trade direction."""
    mult = _pip_multiplier(entry)
    raw = round((close - entry) * mult)
    directional = raw if direction == "BUY" else -raw
    sign = "+" if directional >= 0 else ""
    n = abs(directional)

    if lang == "ru":
        if n % 10 == 1 and n % 100 != 11:
            unit = t("outcome_pts_one", lang)
        elif 2 <= n % 10 <= 4 and not (12 <= n % 100 <= 14):
            unit = t("outcome_pts_few", lang)
        else:
            unit = t("outcome_pts_many", lang)
    else:
        unit = t("outcome_pts_many", lang)

    return f"{sign}{directional} {unit}"


def _strategy_label(strategy: str, lang: str = "ru") -> str:
    """Return a localised strategy name for display."""
    key = {
        "ema_bounce":     "strategy_ema_bounce",
        "level_breakout": "strategy_level_breakout",
        "level_bounce":   "strategy_level_bounce",
        "rsi_reversal":   "strategy_rsi_reversal",
        "impulse":        "strategy_impulse",
        "bounce":         "strategy_bounce",
        "breakout":       "strategy_breakout",
    }.get(strategy)
    return t(key, lang) if key else strategy or "—"


def _build_explanation(outcome: str, direction: str, strategy: str, pct: float, lang: str = "ru") -> str:
    """Return a short plain-text explanation of why the trade won or lost."""
    if outcome == "win":
        if pct >= 0.05:
            return t("outcome_exp_win_strong", lang)
        else:
            return t("outcome_exp_win_marginal", lang)

    # outcome == "loss" — explain by strategy
    loss_keys = {
        "ema_bounce":     "outcome_exp_loss_ema",
        "level_bounce":   "outcome_exp_loss_level",
        "level_breakout": "outcome_exp_loss_breakout",
        "rsi_reversal":   "outcome_exp_loss_rsi",
    }
    reason_key = loss_keys.get(strategy, "outcome_exp_loss_default")
    return t(reason_key, lang) + t("outcome_exp_loss_suffix", lang)


async def track_outcome(
    bot: Bot,
    chat_id: int,
    outcome_id: int,
    symbol: str,
    pair_label: str,
    direction: str,
    strategy: str | None,
    expiration_sec: int,
    signal_price: float,
) -> None:
    """
    Background task: sleep → fetch last candle price → determine result → notify.
    Runs as asyncio.create_task(), all errors caught internally.
    """
    try:
        await asyncio.sleep(expiration_sec + 2)

        lang = get_lang(chat_id)

        # Try WS first (fast, no browser), fall back to cache
        candles = []
        try:
            from services.po_ws_client import fetch_all_pairs, is_available
            if is_available():
                async with asyncio.timeout(15):
                    ws_result = await fetch_all_pairs([symbol])
                candles = ws_result.get(symbol, [])
        except Exception as ws_err:
            logger.debug("WS price fetch for outcome failed: %s — using cache", ws_err)

        if not candles:
            from services.candle_cache import get_cached
            candles = get_cached(symbol) or []

        if not candles:
            raise ValueError("No candles available for outcome check")

        result_price = float(candles[-1]["close"])

        if direction == "BUY":
            outcome = "win" if result_price > signal_price else "loss"
        else:
            outcome = "win" if result_price < signal_price else "loss"

        await resolve_outcome(outcome_id, result_price, outcome)

        pct = abs((result_price - signal_price) / signal_price * 100)

        _signed_pnl = (
            (result_price - signal_price) / signal_price * 100 if direction == "BUY"
            else (signal_price - result_price) / signal_price * 100
        )
        asyncio.create_task(analytics_logger.update_result(
            outcome_id=outcome_id,
            close_price=result_price,
            result=outcome,
            pnl_pct=round(_signed_pnl, 5),
        ))

        # ── Notify SignalFilter + TradeLogger about the outcome ───────────────
        try:
            from services.auto_signal_service import notify_outcome
            notify_outcome(
                pair=pair_label,
                result="WIN" if outcome == "win" else "LOSS",
                pnl=round(_signed_pnl, 5),
                close_price=result_price,
            )
        except Exception as _notify_exc:
            logger.debug("notify_outcome skipped: %s", _notify_exc)

        if outcome == "win":
            icon  = "✅"
            arrow = "⬆️" if direction == "BUY" else "⬇️"
        else:
            icon  = "❌"
            arrow = "⬇️" if direction == "BUY" else "⬆️"

        header        = t("outcome_win_header" if outcome == "win" else "outcome_loss_header", lang)
        strat_label   = _strategy_label(strategy or "", lang)
        exp_label     = t("expiry_min", lang, n=expiration_sec // 60) if expiration_sec >= 60 else t("expiry_sec", lang, n=expiration_sec)
        explanation   = _build_explanation(outcome, direction, strategy or "", pct, lang)
        points_str    = _format_points(signal_price, result_price, direction, lang)

        text = (
            f"{icon} <b>{header}</b>\n"
            f"\n"
            f"📊 <b>{pair_label}</b> · {direction} · {exp_label}\n"
            f"{t('outcome_strategy', lang, label=strat_label)}\n"
            f"\n"
            f"{t('outcome_entry', lang, price=f'{signal_price:.5g}')}\n"
            f"{t('outcome_exit', lang, price=f'{result_price:.5g}', arrow=arrow)}\n"
            f"{t('outcome_diff', lang, points=points_str)}\n"
            f"\n"
            f"<i>{explanation}</i>\n"
        )

        await bot.send_message(
            chat_id,
            text,
            parse_mode="HTML",
            reply_markup=after_result_keyboard(symbol, lang=lang),
        )
        logger.info(
            "Outcome tracked: %s | %s %s | entry=%.6f result=%.6f | %s",
            pair_label, direction, exp_label, signal_price, result_price, outcome.upper(),
        )

    except Exception as e:
        logger.warning("Outcome tracking failed for id=%d: %s", outcome_id, e)
        try:
            await resolve_outcome(outcome_id, 0.0, "error")
        except Exception:
            pass


# ── Startup recovery ──────────────────────────────────────────────────────────

async def _recover_one(
    bot: Bot,
    outcome_id: int,
    symbol: str,
    user_id: int,
    direction: str,
    strategy: str | None,
    expiration_sec: int,
    signal_price: float,
    pair_label: str,
    delay_sec: float,
    stale_sec: float = 0.0,
) -> None:
    """
    Resolve a single pending outcome after an optional delay, then notify user.

    stale_sec: how many seconds have already passed AFTER expiry at restart time.
    If > 60s, the current market price is no longer a reliable proxy for the
    trade close price — we mark the outcome as unknown and ask the user to verify.
    """
    _STALE_THRESHOLD = 60  # seconds past expiry before we stop guessing

    try:
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)

        lang          = get_lang(user_id)
        strat_label   = _strategy_label(strategy or "", lang)
        exp_label     = t("expiry_min", lang, n=expiration_sec // 60) if expiration_sec >= 60 else t("expiry_sec", lang, n=expiration_sec)

        # ── Trade expired long before bot restarted: result unknowable ─────────
        if stale_sec > _STALE_THRESHOLD:
            await resolve_outcome(outcome_id, 0.0, "error")
            text = (
                f"{t('outcome_stale_header', lang, pair=pair_label)}\n"
                f"\n"
                f"📊 <b>{pair_label}</b> · {direction} · {exp_label}\n"
                f"{t('outcome_strategy', lang, label=strat_label)}\n"
                f"\n"
                f"{t('outcome_stale_body', lang, n=int(stale_sec))}\n"
                f"\n"
                f"{t('outcome_stale_hint', lang)}"
            )
            await bot.send_message(
                user_id, text,
                parse_mode="HTML",
                reply_markup=after_result_keyboard(symbol, lang=lang),
            )
            logger.info(
                "recover_one: stale=%ds → unknown id=%d (%s %s)",
                int(stale_sec), outcome_id, direction, pair_label,
            )
            return

        # ── Freshly expired (or still live): determine result from price ───────
        candles = []
        try:
            from services.po_ws_client import fetch_all_pairs, is_available
            if is_available():
                async with asyncio.timeout(15):
                    ws_result = await fetch_all_pairs([symbol])
                candles = ws_result.get(symbol, [])
        except Exception:
            pass

        if not candles:
            from services.candle_cache import get_cached
            candles = get_cached(symbol) or []

        if not candles:
            logger.warning("recover_one: no candles for %s, marking error", symbol)
            await resolve_outcome(outcome_id, 0.0, "error")
            return

        result_price = float(candles[-1]["close"])

        if direction == "BUY":
            outcome = "win" if result_price > signal_price else "loss"
        else:
            outcome = "win" if result_price < signal_price else "loss"

        await resolve_outcome(outcome_id, result_price, outcome)

        pct = abs((result_price - signal_price) / signal_price * 100)
        _signed_pnl = (
            (result_price - signal_price) / signal_price * 100 if direction == "BUY"
            else (signal_price - result_price) / signal_price * 100
        )
        asyncio.create_task(analytics_logger.update_result(
            outcome_id=outcome_id,
            close_price=result_price,
            result=outcome,
            pnl_pct=round(_signed_pnl, 5),
        ))

        if outcome == "win":
            icon  = "✅"
            arrow = "⬆️" if direction == "BUY" else "⬇️"
        else:
            icon  = "❌"
            arrow = "⬇️" if direction == "BUY" else "⬆️"

        header      = t("outcome_win_header" if outcome == "win" else "outcome_loss_header", lang)
        explanation = _build_explanation(outcome, direction, strategy or "", pct, lang)
        points_str  = _format_points(signal_price, result_price, direction, lang)

        text = (
            f"{icon} <b>{header}</b>\n"
            f"\n"
            f"📊 <b>{pair_label}</b> · {direction} · {exp_label}\n"
            f"{t('outcome_strategy', lang, label=strat_label)}\n"
            f"\n"
            f"{t('outcome_entry', lang, price=f'{signal_price:.5g}')}\n"
            f"{t('outcome_exit', lang, price=f'{result_price:.5g}', arrow=arrow)}\n"
            f"{t('outcome_diff', lang, points=points_str)}\n"
            f"\n"
            f"<i>{explanation}</i>\n"
            f"<i>{t('outcome_recovered', lang)}</i>"
        )

        await bot.send_message(
            user_id, text,
            parse_mode="HTML",
            reply_markup=after_result_keyboard(symbol, lang=lang),
        )
        logger.info(
            "recover_one: outcome=%s id=%d (%s %s)", outcome, outcome_id, direction, pair_label
        )

    except Exception as e:
        logger.warning("recover_one failed for id=%d: %s", outcome_id, e)
        try:
            await resolve_outcome(outcome_id, 0.0, "error")
        except Exception:
            pass


async def recover_pending_outcomes(bot: Bot) -> None:
    """
    Called once at bot startup. Finds all pending signal outcomes from the
    last 30 minutes (could be lost due to restart) and reschedules tracking.
    """
    from db.database import get_pending_outcomes
    from datetime import datetime, timezone

    try:
        pending = await get_pending_outcomes(max_age_sec=1800)
        if not pending:
            logger.info("recover_pending_outcomes: nothing to recover")
            return

        logger.info("recover_pending_outcomes: recovering %d pending outcome(s)", len(pending))

        for row in pending:
            try:
                created_at = datetime.fromisoformat(row["created_at"]).replace(tzinfo=timezone.utc)
                now        = datetime.now(timezone.utc)
                age_sec    = (now - created_at).total_seconds()
                delay_sec  = max(0.0, row["expiration_sec"] - age_sec + 2)
                stale_sec  = max(0.0, age_sec - row["expiration_sec"])
            except Exception:
                delay_sec = 0.0
                stale_sec = 0.0

            asyncio.create_task(_recover_one(
                bot           = bot,
                outcome_id    = row["id"],
                symbol        = row["symbol"],
                user_id       = row["user_id"],
                direction     = row["direction"],
                strategy      = row.get("strategy"),
                expiration_sec= row["expiration_sec"],
                signal_price  = row["signal_price"],
                pair_label    = row["pair_label"],
                delay_sec     = delay_sec,
                stale_sec     = stale_sec,
            ))

    except Exception as e:
        logger.warning("recover_pending_outcomes failed: %s", e)
