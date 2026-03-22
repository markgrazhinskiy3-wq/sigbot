import asyncio
import logging
import time as _time

from aiogram import Bot
from aiogram.types import FSInputFile

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from services.pocket_browser import place_demo_trade, take_trade_result_screenshot
from services.signal_service import format_result_caption
from db.database import resolve_outcome
from bot.keyboards import after_result_keyboard

logger = logging.getLogger(__name__)


async def watch_and_report(
    bot: Bot,
    chat_id: int,
    symbol: str,
    pair_label: str,
    expiration_sec: int,
    direction: str,
    details: dict,
    outcome_id: int | None = None,
) -> None:
    """
    Places a $1 demo trade immediately, waits for expiration,
    then screenshots the result and sends it with analysis explanation.
    If outcome_id is given, resolves the DB record after the trade closes.
    """
    logger.info(
        "Watcher started: %s | exp=%ds | user=%d | dir=%s",
        symbol, expiration_sec, chat_id, direction,
    )

    placed_at: float = 0
    try:
        await place_demo_trade(symbol, direction, expiration_sec)
        placed_at = _time.time()
        logger.info("Demo trade placed for %s %s", direction, symbol)
    except Exception as e:
        logger.exception("Failed to place demo trade: %s", e)
        if outcome_id is not None:
            try:
                await resolve_outcome(outcome_id, 0.0, "error")
            except Exception:
                pass
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"⚠️ <b>Не удалось поставить сделку по {pair_label}</b>\n\n"
                    f"Пара не переключилась на платформе — сделка <b>не была открыта</b>.\n"
                    f"Попробуйте запросить сигнал ещё раз."
                ),
                parse_mode="HTML",
                reply_markup=after_result_keyboard(symbol),
            )
        except Exception:
            pass
        return

    await asyncio.sleep(expiration_sec + 1)

    screenshot_path: str | None = None
    trade_outcome: str = "unknown"

    try:
        screenshot_path, trade_outcome = await take_trade_result_screenshot(
            symbol, direction,
            placed_at=placed_at,
            expiration_sec=expiration_sec,
        )

        if outcome_id is not None and trade_outcome in ("win", "loss"):
            try:
                await resolve_outcome(outcome_id, 0.0, trade_outcome)
                logger.info("Outcome resolved: id=%d → %s", outcome_id, trade_outcome)
            except Exception as e:
                logger.warning("resolve_outcome failed: %s", e)

        caption = format_result_caption(pair_label, direction, expiration_sec, details, trade_outcome)

        photo = FSInputFile(screenshot_path)
        await bot.send_photo(chat_id=chat_id, photo=photo, caption=caption, parse_mode="HTML")
        logger.info("Result screenshot sent to user %d", chat_id)

        await bot.send_message(
            chat_id=chat_id,
            text="Что делаем дальше?",
            reply_markup=after_result_keyboard(symbol),
        )

    except Exception as e:
        logger.exception("Result watcher failed for user %d: %s", chat_id, e)
        if outcome_id is not None and trade_outcome not in ("win", "loss"):
            try:
                await resolve_outcome(outcome_id, 0.0, "error")
            except Exception:
                pass
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "⚠️ Не удалось сделать скриншот результата.\n"
                    "Проверьте результат самостоятельно на платформе."
                ),
                reply_markup=after_result_keyboard(symbol),
            )
        except Exception:
            pass


def schedule_result_watcher(
    bot: Bot,
    chat_id: int,
    symbol: str,
    pair_label: str,
    expiration_sec: int,
    direction: str,
    details: dict,
    outcome_id: int | None = None,
) -> asyncio.Task:
    task = asyncio.create_task(
        watch_and_report(
            bot, chat_id, symbol, pair_label,
            expiration_sec, direction, details,
            outcome_id=outcome_id,
        )
    )
    logger.info("Result watcher task created for user %d", chat_id)
    return task
