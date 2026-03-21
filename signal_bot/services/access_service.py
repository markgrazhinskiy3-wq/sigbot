import logging
from aiogram import Bot

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
from db.database import add_or_get_user, get_status

logger = logging.getLogger(__name__)


async def register_and_check(
    bot: Bot, user_id: int, username: str | None
) -> str:
    """
    Register user if new, notify admin if first visit.
    Returns current status: 'pending' | 'approved' | 'denied'
    """
    user = await add_or_get_user(user_id, username)
    status = user["status"]

    if status == "pending" and user.get("just_created", False) is False:
        is_new = await _is_newly_created(user)
        if is_new:
            await _notify_admin(bot, user_id, username)

    return status


async def _is_newly_created(user: dict) -> bool:
    from datetime import datetime, timedelta
    try:
        created = datetime.fromisoformat(user["created_at"])
        return (datetime.utcnow() - created).total_seconds() < 5
    except Exception:
        return False


async def _notify_admin(bot: Bot, user_id: int, username: str | None) -> None:
    uname = f"@{username}" if username else f"ID:{user_id}"
    text = (
        f"🆕 <b>Новый запрос на доступ</b>\n\n"
        f"Пользователь: {uname}\n"
        f"ID: <code>{user_id}</code>\n\n"
        f"Одобрить: /approve {user_id}\n"
        f"Отклонить: /deny {user_id}"
    )
    try:
        await bot.send_message(config.ADMIN_USER_ID, text, parse_mode="HTML")
        logger.info("Admin notified about new user %s", user_id)
    except Exception as e:
        logger.error("Failed to notify admin: %s", e)


async def check_access(user_id: int) -> str:
    """Returns 'approved' | 'pending' | 'denied' | 'unknown'"""
    status = await get_status(user_id)
    return status or "unknown"
