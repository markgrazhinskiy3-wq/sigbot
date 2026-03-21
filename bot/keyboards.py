from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder


def main_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📅 Сегодня", callback_data="period:today"),
        InlineKeyboardButton(text="📅 Вчера", callback_data="period:yesterday"),
    )
    builder.row(
        InlineKeyboardButton(
            text="📅 Последние 7 дней", callback_data="period:last_7"
        ),
    )
    builder.row(
        InlineKeyboardButton(
            text="📅 Последние 30 дней", callback_data="period:last_30"
        ),
    )
    builder.row(
        InlineKeyboardButton(
            text="✏️ Свободный период", callback_data="period:custom"
        ),
    )
    return builder.as_markup()


def cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"),
    )
    return builder.as_markup()
