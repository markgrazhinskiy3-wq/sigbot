from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config


def main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📈 Получить сигнал", callback_data="action:get_signal"),
    )
    builder.row(
        InlineKeyboardButton(text="🔁 Перезапустить бота", callback_data="action:restart_bot"),
    )
    return builder.as_markup()


def pairs_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for pair in config.OTC_PAIRS:
        builder.row(
            InlineKeyboardButton(
                text=pair["label"],
                callback_data=f"pair:{pair['symbol']}",
            )
        )
    builder.row(
        InlineKeyboardButton(text="⬅️ Назад", callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def expiration_keyboard(symbol: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for exp in config.EXPIRATIONS:
        builder.row(
            InlineKeyboardButton(
                text=exp["label"],
                callback_data=f"exp:{symbol}:{exp['seconds']}",
            )
        )
    builder.row(
        InlineKeyboardButton(text="⬅️ Назад", callback_data="action:get_signal"),
    )
    return builder.as_markup()


def back_to_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def after_result_keyboard(symbol: str) -> InlineKeyboardMarkup:
    """Keyboard shown after a trade result arrives."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="🔄 Следующий сигнал",
            callback_data=f"pair:{symbol}",  # re-uses pair handler → shows expiration picker
        )
    )
    builder.row(
        InlineKeyboardButton(text="📊 Выбрать пару", callback_data="action:get_signal"),
    )
    builder.row(
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="action:back_to_menu"),
    )
    return builder.as_markup()
