import logging
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
from db.database import get_status

logger = logging.getLogger(__name__)


def _admin_approval_keyboard(user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin:approve:{user_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin:deny:{user_id}"),
    )
    return builder.as_markup()


async def notify_admin_new_user(bot: Bot, user_id: int, username: str | None) -> None:
    """Send admin a notification with approve/deny buttons."""
    uname = f"@{username}" if username else f"ID:{user_id}"
    text = (
        f"🆕 <b>Новый запрос на доступ</b>\n\n"
        f"Пользователь: {uname}\n"
        f"ID: <code>{user_id}</code>"
    )
    try:
        await bot.send_message(
            config.ADMIN_USER_ID,
            text,
            parse_mode="HTML",
            reply_markup=_admin_approval_keyboard(user_id),
        )
        logger.info("Admin notified about new user %s (%s)", uname, user_id)
    except Exception as e:
        logger.error("Failed to notify admin about user %s: %s", user_id, e)


async def check_access(user_id: int) -> str:
    """Returns 'approved' | 'pending' | 'denied' | 'unknown'"""
    status = await get_status(user_id)
    return status or "unknown"
