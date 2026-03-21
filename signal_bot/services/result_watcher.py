import asyncio
import logging
import time as _time

from aiogram import Bot
from aiogram.types import FSInputFile

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from services.pocket_browser import place_demo_trade, take_trade_result_screenshot
from services.signal_service import format_result_caption
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
) -> None:
    """
    Places a $1 demo trade immediately, waits for expiration,
    then screenshots the result and sends it with analysis explanation.
    """
    logger.info(
        "Watcher started: %s | exp=%ds | user=%d | dir=%s",
        symbol, expiration_sec, chat_id, direction,
    )

    placed_at: float = 0
    try:
        await place_demo_trade(symbol, direction, expiration_sec)
        placed_at = _time.time()   # record AFTER successful placement
        logger.info("Demo trade placed for %s %s", direction, symbol)
    except Exception as e:
        logger.exception("Failed to place demo trade: %s", e)

    await asyncio.sleep(expiration_sec + 5)

    try:
        screenshot_path, outcome = await take_trade_result_screenshot(
            symbol, direction,
            placed_at=placed_at,
            expiration_sec=expiration_sec,
        )

        caption = format_result_caption(pair_label, direction, expiration_sec, details, outcome)

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
) -> asyncio.Task:
    task = asyncio.create_task(
        watch_and_report(bot, chat_id, symbol, pair_label, expiration_sec, direction, details)
    )
    logger.info("Result watcher task created for user %d", chat_id)
    return task
