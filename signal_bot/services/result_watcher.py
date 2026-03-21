import asyncio
import logging

from aiogram import Bot
from aiogram.types import FSInputFile

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from services.pocket_browser import take_screenshot

logger = logging.getLogger(__name__)


async def watch_and_report(
    bot: Bot,
    chat_id: int,
    symbol: str,
    pair_label: str,
    expiration_sec: int,
    direction: str,
) -> None:
    """
    Waits for expiration, then takes a screenshot and sends it to the user.
    Runs as a background asyncio task.
    """
    logger.info(
        "Watcher started: %s | exp=%ds | user=%d", symbol, expiration_sec, chat_id
    )

    await asyncio.sleep(expiration_sec + 3)

    try:
        screenshot_path = await take_screenshot(symbol)

        arrow = "🟢" if direction == "BUY" else "🔴"
        caption = (
            f"{arrow} <b>{pair_label}</b> — результат\n"
            f"Ваш сигнал: <b>{direction}</b>\n"
            f"Экспирация: {expiration_sec} сек"
        )

        photo = FSInputFile(screenshot_path)
        await bot.send_photo(chat_id=chat_id, photo=photo, caption=caption, parse_mode="HTML")
        logger.info("Result screenshot sent to user %d", chat_id)

    except Exception as e:
        logger.exception("Result watcher failed for user %d: %s", chat_id, e)
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "⚠️ Не удалось сделать скриншот результата.\n"
                    "Проверьте результат самостоятельно на платформе."
                ),
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
) -> asyncio.Task:
    """
    Schedule the watcher as a background asyncio task.
    """
    task = asyncio.create_task(
        watch_and_report(bot, chat_id, symbol, pair_label, expiration_sec, direction)
    )
    logger.info("Result watcher task created for user %d", chat_id)
    return task
