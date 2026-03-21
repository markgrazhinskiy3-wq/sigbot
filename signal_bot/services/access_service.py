import logging
from aiogram import Bot

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
from db.database import get_status

logger = logging.getLogger(__name__)


async def notify_admin_new_user(bot: Bot, user_id: int, username: str | None) -> None:
    """Send admin a notification about a newly registered user."""
    uname = f"@{username}" if username else f"ID:{user_id}"
    text = (
        f"🆕 <b>Новый запрос на доступ</b>\n\n"
        f"Пользователь: {uname}\n"
        f"ID: <code>{user_id}</code>\n\n"
        f"✅ Одобрить: /approve {user_id}\n"
        f"❌ Отклонить: /deny {user_id}"
    )
    try:
        await bot.send_message(config.ADMIN_USER_ID, text, parse_mode="HTML")
        logger.info("Admin notified about new user %s (%s)", uname, user_id)
    except Exception as e:
        logger.error("Failed to notify admin about user %s: %s", user_id, e)


async def check_access(user_id: int) -> str:
    """Returns 'approved' | 'pending' | 'denied' | 'unknown'"""
    status = await get_status(user_id)
    return status or "unknown"
