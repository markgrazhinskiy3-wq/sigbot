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
        await asyncio.sleep(expiration_sec + 3)

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

        text = (
            f"{icon} <b>{header}</b>\n"
            f"\n"
            f"📊 <b>{pair_label}</b> · {direction} · {exp_label}\n"
            f"💡 Стратегия: {strategy_label}\n"
            f"\n"
            f"Цена входа:  <code>{signal_price:.5g}</code>\n"
            f"Цена выхода: <code>{result_price:.5g}</code> {arrow}\n"
            f"Изменение:   {pct:.3f}%\n"
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
