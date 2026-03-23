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
from aiogram.types import BotCommand, BotCommandScopeDefault, BotCommandScopeChat

import config
from db.database import init_db
from bot.handlers import router, load_admin_cache
from services.pocket_browser import close_browser, init_monitor_ws_auth
from services.candle_cache import start_refresher
from services.strategy_adaptation import initialize as init_strategy_adaptation
from services.outcome_tracker import recover_pending_outcomes
import services.pairs_cache as pairs_cache

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


async def _setup_commands(bot: Bot) -> None:
    user_commands = [
        BotCommand(command="signal", description="📊 Скан пар — лучшие сигналы прямо сейчас"),
        BotCommand(command="start",  description="🏠 Главное меню"),
        BotCommand(command="stats",  description="📈 Моя статистика"),
        BotCommand(command="help",   description="ℹ️ Как пользоваться ботом"),
    ]
    admin_commands = user_commands + [
        BotCommand(command="admin",     description="⚙️ Панель администратора"),
        BotCommand(command="pending",   description="⏳ Заявки на одобрение"),
        BotCommand(command="approve",   description="✅ Одобрить пользователя"),
        BotCommand(command="deny",      description="❌ Отклонить пользователя"),
        BotCommand(command="users",     description="👥 Список всех пользователей"),
        BotCommand(command="broadcast", description="📢 Рассылка"),
        BotCommand(command="debug",     description="🔬 Debug анализа пары"),
        BotCommand(command="daystats",  description="📅 Статистика всех сигналов за сегодня"),
        BotCommand(command="report",    description="📋 Performance-отчёт по стратегиям (/report 7)"),
        BotCommand(command="condstats", description="🔬 Частота условий стратегий (/condstats reset)"),
        BotCommand(command="addadmin",     description="➕ Назначить администратора"),
        BotCommand(command="removeadmin",  description="➖ Снять администратора"),
        BotCommand(command="admins",       description="👥 Список администраторов"),
    ]

    await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())
    try:
        await bot.set_my_commands(
            admin_commands,
            scope=BotCommandScopeChat(chat_id=config.ADMIN_USER_ID),
        )
    except Exception as e:
        logger.warning("Could not set admin commands scope: %s", e)

    logger.info("Bot command menu registered (%d user + %d admin commands)",
                len(user_commands), len(admin_commands))


async def main() -> None:
    _kill_stale_instances()

    logger.info("Initializing Signal Bot database...")
    await init_db()

    logger.info("Loading admin cache...")
    await load_admin_cache()

    logger.info("Initializing strategy adaptation module...")
    await init_strategy_adaptation()

    logger.info("Starting Pocket Option Signal Bot")

    # Fetch live OTC pairs from PocketOption (payout ≥ 82% for recommended, ≥ 85% for manual list).
    # This runs BEFORE candle warm-up so the refresher gets the real list.
    # Falls back to config.OTC_PAIRS automatically if browser fetch fails.
    logger.info("Fetching live OTC pairs from PocketOption (payout ≥ 82%%)...")
    try:
        live_pairs = await pairs_cache.refresh(force=True)
        logger.info("Live pairs loaded: %d pairs", len(live_pairs))
    except Exception as e:
        logger.warning("Could not fetch live pairs, using static config: %s", e)
        live_pairs = None   # start_refresher will fall back to config.OTC_PAIRS

    # Start candle cache — warms up all live pairs in background so signal
    # requests are served instantly from cache instead of opening a browser per user.
    start_refresher(pairs=live_pairs)

    # Initialise secondary account WS auth for real-time monitoring.
    # After it completes, refresh pairs cache so live payouts get picked up.
    async def _init_monitor_then_refresh():
        await init_monitor_ws_auth()
        logger.info("Monitor auth done — re-refreshing pairs cache with live payouts…")
        try:
            await pairs_cache.refresh(force=True)
            logger.info("Pairs cache re-refreshed after monitor auth: %d pairs", len(pairs_cache.get_cached()))
        except Exception as e:
            logger.warning("Post-monitor pairs refresh failed: %s", e)

    asyncio.create_task(_init_monitor_then_refresh())

    bot = Bot(
        token=config.TELEGRAM_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    await _setup_commands(bot)

    # Recover any pending outcomes lost due to previous restart
    asyncio.create_task(recover_pending_outcomes(bot))

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
