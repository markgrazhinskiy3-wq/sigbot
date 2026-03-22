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
from services.pocket_browser import get_candles
from bot.keyboards import after_result_keyboard

logger = logging.getLogger(__name__)

_STRATEGY_LABELS = {
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
        await asyncio.sleep(expiration_sec + 5)

        candles = await get_candles(symbol, count=5)
        if not candles:
            raise ValueError("No candles returned")

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
