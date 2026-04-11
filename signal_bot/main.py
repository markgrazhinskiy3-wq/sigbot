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
from db.database import init_db, load_all_user_langs
from bot.handlers import router, load_admin_cache
from bot.i18n import load_langs_from_db
from services.pocket_browser import close_browser, init_monitor_ws_auth
from services.candle_cache import start_refresher
from services.strategy_adaptation import initialize as init_strategy_adaptation
from services.outcome_tracker import recover_pending_outcomes
from services.auto_signal_service import auto_signal_loop
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
        BotCommand(command="stats",  description="📈 Моя статистика — WIN/LOSS и winrate"),
        BotCommand(command="help",   description="ℹ️ Как пользоваться ботом"),
    ]
    admin_commands = user_commands + [
        # ── Пользователи ───────────────────────────────────────────────────
        BotCommand(command="pending",    description="⏳ Заявки на одобрение"),
        BotCommand(command="approve",    description="✅ Одобрить: /approve ID"),
        BotCommand(command="deny",       description="❌ Отклонить: /deny ID"),
        BotCommand(command="users",      description="👥 Все пользователи и статусы"),
        BotCommand(command="broadcast",  description="📢 Рассылка: /broadcast текст"),
        # ── Анализ и отчёты ────────────────────────────────────────────────
        BotCommand(command="debug",      description="🔬 Анализ: /debug или /debug SYMBOL"),
        BotCommand(command="report",     description="📋 Отчёт: /report today | 7 | 30 | all"),
        BotCommand(command="condstats",  description="📉 Частота условий стратегий"),
        BotCommand(command="export_csv", description="💾 Экспорт логов сигналов в CSV"),
        # ── Бэктест ────────────────────────────────────────────────────────
        BotCommand(command="paper_test", description="🧪 Бэктест: /paper_test 50 | 50 stoch | stop"),
        # ── Пары и WS ──────────────────────────────────────────────────────
        BotCommand(command="pairsinfo",  description="📡 Все пары с payout%"),
        BotCommand(command="pairsdiag",  description="🔌 Диагностика WebSocket PocketOption"),
        # ── Управление администраторами ────────────────────────────────────
        BotCommand(command="addadmin",    description="➕ Назначить администратора: /addadmin ID"),
        BotCommand(command="removeadmin", description="➖ Снять администратора: /removeadmin ID"),
        BotCommand(command="admins",      description="👥 Список всех администраторов"),
        BotCommand(command="update_ssid", description="🔑 Обновить WS токен PocketOption"),
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

    logger.info("Loading user language preferences...")
    _langs = await load_all_user_langs()
    load_langs_from_db(_langs)
    logger.info("Loaded language preferences for %d user(s)", len(_langs))

    logger.info("Initializing strategy adaptation module...")
    await init_strategy_adaptation()

    logger.info("Starting Pocket Option Signal Bot")

    # Apply SSID from env var BEFORE the candle refresher starts so the
    # WS client immediately has valid credentials without a browser login.
    from services.po_ws_client import (
        apply_ssid_from_env, refresh_session_via_worker, session_refresh_loop,
    )
    if apply_ssid_from_env():
        logger.info("PO_SSID env var applied — WS auth ready without browser login")
    else:
        logger.info("PO_SSID not set — will use saved WS auth file or browser login")

    # If using Cloudflare Worker proxy: get a fresh CF-IP session so WS auth works.
    # The PO_SSID session is tied to the user's home IP; CF Worker has a different IP.
    cf_ok = await refresh_session_via_worker()
    if cf_ok:
        logger.info("CF session refresh done — WS auth updated with Cloudflare-IP session")
    else:
        logger.info("CF session refresh skipped (not using Cloudflare Worker or no credentials)")

    # Periodically refresh CF session in background (every 2 hours)
    asyncio.create_task(session_refresh_loop(interval_seconds=7200))

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

    # ── Optional candle probe (set PROBE_CANDLES=1 env var on Railway) ─────────
    if os.environ.get("PROBE_CANDLES") == "1":
        async def _run_probe():
            from services.po_ws_client import is_available
            logger.info("[PROBE] Waiting for WS auth to run candle probe...")
            for _ in range(120):
                if is_available():
                    break
                await asyncio.sleep(5)
            else:
                logger.warning("[PROBE] WS auth never became available — skipping probe")
                return
            try:
                import importlib.util, pathlib
                spec = importlib.util.spec_from_file_location(
                    "probe_candles",
                    pathlib.Path(__file__).parent / "probe_candles.py",
                )
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                await mod.main()
            except Exception as exc:
                logger.exception("[PROBE] probe_candles.py failed: %s", exc)
        asyncio.create_task(_run_probe())
    # ────────────────────────────────────────────────────────────────────────────

    bot = Bot(
        token=config.TELEGRAM_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    await _setup_commands(bot)

    # Recover any pending outcomes lost due to previous restart
    asyncio.create_task(recover_pending_outcomes(bot))

    # Start auto-signal broadcaster (waits 2 min internally for candle warm-up)
    asyncio.create_task(auto_signal_loop(bot))

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
