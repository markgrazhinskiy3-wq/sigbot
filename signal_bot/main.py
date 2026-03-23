import asyncio
import logging
import signal
import sys
import os
import subprocess

sys.path.insert(0, os.path.dirname(__file__))

# ── Ensure Playwright Chromium is installed (non-blocking background process) ─
# Railway may not preserve the browser cache between redeploys even if the
# Dockerfile has `playwright install chromium`. We start the install as a
# background process so the bot responds immediately; by the time
# init_monitor_ws_auth() runs (~30s later), browsers will be ready.
_pw_install_proc = None
try:
    import shutil
    _pw_bin = shutil.which("playwright") or "playwright"
    _pw_install_proc = subprocess.Popen(
        [_pw_bin, "install", "chromium"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    print(f"[startup] playwright install chromium started (pid={_pw_install_proc.pid})", flush=True)
except Exception as _e:
    print(f"[startup] Could not start playwright install: {_e}", flush=True)

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

    # Load OTC pairs instantly from config (no browser needed)
    live_pairs = await pairs_cache.refresh(force=True)
    logger.info("OTC pairs loaded: %d pairs", len(live_pairs))

    # Start candle cache — warms up all pairs in background
    start_refresher(pairs=live_pairs)

    # Initialise WS auth for real-time monitoring (background, non-blocking)
    async def _init_monitor():
        if _pw_install_proc is not None:
            try:
                await asyncio.get_event_loop().run_in_executor(None, _pw_install_proc.wait)
                rc = _pw_install_proc.returncode
                if rc == 0:
                    logger.info("Playwright chromium ready")
                else:
                    stderr = _pw_install_proc.stderr.read() if _pw_install_proc.stderr else b""
                    logger.warning("playwright install exited %d: %s", rc, stderr.decode()[:200])
            except Exception as e:
                logger.warning("Could not wait for playwright install: %s", e)
        await init_monitor_ws_auth()
        logger.info("Monitor WS auth done")

    asyncio.create_task(_init_monitor())

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
