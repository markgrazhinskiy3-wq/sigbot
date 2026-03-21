import asyncio
import logging
import signal
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

import config
from db.database import init_db
from bot.handlers import router
from services.pocket_browser import close_browser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def _kill_stale_instances() -> None:
    """Kill any other running instances of this script to avoid token conflicts."""
    import subprocess
    my_pid = os.getpid()
    try:
        result = subprocess.run(
            ["pgrep", "-f", "signal_bot/main.py"],
            capture_output=True, text=True
        )
        for pid_str in result.stdout.strip().splitlines():
            pid = int(pid_str.strip())
            if pid != my_pid:
                try:
                    os.kill(pid, signal.SIGTERM)
                    logger.info("Terminated stale instance PID %d", pid)
                except ProcessLookupError:
                    pass
    except Exception as e:
        logger.warning("Could not check for stale instances: %s", e)


async def main() -> None:
    _kill_stale_instances()

    logger.info("Initializing Signal Bot database...")
    await init_db()

    logger.info("Starting Pocket Option Signal Bot")

    bot = Bot(
        token=config.TELEGRAM_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    try:
        logger.info("Bot started. Admin ID: %d", config.ADMIN_USER_ID)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        logger.info("Shutting down, closing browser...")
        await close_browser()
        await bot.session.close()
        logger.info("Signal Bot stopped")


if __name__ == "__main__":
    asyncio.run(main())
