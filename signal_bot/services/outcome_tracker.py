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
from services import analytics_logger

logger = logging.getLogger(__name__)

_STRATEGY_LABELS = {
    # new engine strategies
    "ema_bounce":       "Отскок от EMA",
    "squeeze_breakout": "Пробой сжатия",
    "level_bounce":     "Отскок от уровня",
    "rsi_reversal":     "Разворот RSI",
    "micro_breakout":   "Пробой уровня",
    "divergence":       "Дивергенция",
    # legacy (old engine)
    "impulse":  "Импульс по тренду",
    "bounce":   "Отскок от уровня",
    "breakout": "Ложный пробой",
}


def _build_explanation(outcome: str, direction: str, strategy: str, pct: float) -> str:
    """Return a short plain-text explanation of why the trade won or lost."""
    if outcome == "win":
        if pct >= 0.05:
            return "Цена уверенно пошла в нужную сторону — сигнал отработал чисто."
        else:
            return "Цена едва сдвинулась в нужную сторону — победа по минимуму."

    # outcome == "loss" — explain by strategy
    strategy_reasons = {
        "ema_bounce":       "Цена не удержалась у скользящей средней — рынок продолжил движение против сигнала.",
        "squeeze_breakout": "Пробой не получил продолжения — возможно это был ложный пробой.",
        "level_bounce":     "Уровень не удержал цену — давление оказалось сильнее.",
        "rsi_reversal":     "Разворот не состоялся — импульс продолжился в старом направлении.",
        "micro_breakout":   "Пробой уровня развернулся обратно — рынок не подтвердил движение.",
        "divergence":       "Дивергенция не отработала на данной экспирации — нужно больше времени.",
    }
    reason = strategy_reasons.get(strategy, "Цена пошла против сигнала.")

    suffix = (
        " На короткой экспирации даже точный прогноз иногда не срабатывает — одна свеча может всё изменить."
        if strategy not in ("divergence",) else
        " Попробуйте более длинную экспирацию для этой стратегии."
    )
    return reason + suffix


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

        # signed pnl: positive = direction correct
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
            icon   = "✅"
            header = "Сделка закрылась в плюс!"
            arrow  = "⬆️" if direction == "BUY" else "⬇️"
        else:
            icon   = "❌"
            header = "Сделка закрылась в минус."
            arrow  = "⬇️" if direction == "BUY" else "⬆️"

        strategy_label = _STRATEGY_LABELS.get(strategy or "", strategy or "—")
        exp_label = f"{expiration_sec // 60} мин" if expiration_sec >= 60 else f"{expiration_sec} сек"
        explanation = _build_explanation(outcome, direction, strategy or "", pct)

        text = (
            f"{icon} <b>{header}</b>\n"
            f"\n"
            f"📊 <b>{pair_label}</b> · {direction} · {exp_label}\n"
            f"💡 Стратегия: {strategy_label}\n"
            f"\n"
            f"Цена входа:  <code>{signal_price:.5g}</code>\n"
            f"Цена выхода: <code>{result_price:.5g}</code> {arrow}\n"
            f"Изменение:   {pct:.3f}%\n"
            f"\n"
            f"<i>{explanation}</i>\n"
        )

        await bot.send_message(
            chat_id,
            text,
            parse_mode="HTML",
            reply_markup=after_result_keyboard(symbol),
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
) -> None:
    """Resolve a single pending outcome after an optional delay, then notify user."""
    try:
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)

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
        icon   = "✅" if outcome == "win" else "❌"
        header = "Сделка закрылась в плюс!" if outcome == "win" else "Сделка закрылась в минус."
        if outcome == "win":
            arrow = "⬆️" if direction == "BUY" else "⬇️"
        else:
            arrow = "⬇️" if direction == "BUY" else "⬆️"

        strategy_label = _STRATEGY_LABELS.get(strategy or "", strategy or "—")
        exp_label  = f"{expiration_sec // 60} мин" if expiration_sec >= 60 else f"{expiration_sec} сек"
        explanation = _build_explanation(outcome, direction, strategy or "", pct)

        text = (
            f"{icon} <b>{header}</b>\n"
            f"\n"
            f"📊 <b>{pair_label}</b> · {direction} · {exp_label}\n"
            f"💡 Стратегия: {strategy_label}\n"
            f"\n"
            f"Цена входа:  <code>{signal_price:.5g}</code>\n"
            f"Цена выхода: <code>{result_price:.5g}</code> {arrow}\n"
            f"Изменение:   {pct:.3f}%\n"
            f"\n"
            f"<i>{explanation}</i>\n"
            f"<i>⚠️ Результат восстановлен после перезапуска бота.</i>"
        )

        await bot.send_message(
            user_id, text,
            parse_mode="HTML",
            reply_markup=after_result_keyboard(symbol),
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
                # How many seconds left until expiry (may be negative = already expired)
                delay_sec  = max(0.0, row["expiration_sec"] - age_sec + 2)
            except Exception:
                delay_sec = 0.0

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
            ))

    except Exception as e:
        logger.warning("recover_pending_outcomes failed: %s", e)
