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
        InlineKeyboardButton(text="📊 Рекомендуемые пары", callback_data="action:recommended_pairs"),
    )
    builder.row(
        InlineKeyboardButton(text="🔁 Перезапустить бота", callback_data="action:restart_bot"),
    )
    return builder.as_markup()


def pairs_keyboard(pairs: list[dict] | None = None) -> InlineKeyboardMarkup:
    """
    Build the pair selection keyboard.
    `pairs` is a list of {"label": str, "symbol": str}.
    Falls back to static config.OTC_PAIRS when pairs is None.
    """
    builder = InlineKeyboardBuilder()
    source = pairs if pairs is not None else config.OTC_PAIRS
    for pair in source:
        builder.row(
            InlineKeyboardButton(
                text=pair["label"],
                callback_data=f"pair:{pair['symbol']}",
            )
        )
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить список", callback_data="action:refresh_pairs"),
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


def no_signal_keyboard(symbol: str, expiration_sec: int) -> InlineKeyboardMarkup:
    """Keyboard shown when no signal could be generated."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="📊 Рекомендуемые пары",
            callback_data="action:recommended_pairs",
        )
    )
    builder.row(
        InlineKeyboardButton(text="🔀 Выбрать другую пару", callback_data="action:get_signal"),
    )
    builder.row(
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def recommended_pairs_keyboard(signals: list) -> InlineKeyboardMarkup:
    """
    Keyboard with pairs that currently have a BUY/SELL signal.
    Each button shows direction + pair name, clicking opens expiration picker.
    signals: list of SignalResponse sorted by confidence desc.
    """
    builder = InlineKeyboardBuilder()
    for sig in signals:
        builder.row(
            InlineKeyboardButton(
                text=sig.pair,
                callback_data=f"pair:{sig.symbol}",
            )
        )
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="action:recommended_pairs"),
    )
    builder.row(
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def signal_result_keyboard(symbol: str, expiration_sec: int = 0) -> InlineKeyboardMarkup:
    """
    Keyboard shown after a BUY/SELL signal.
    - Next signal: re-opens the expiration picker for the same pair
    - Main menu
    """
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="🔄 Следующий сигнал",
            callback_data=f"pair:{symbol}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="📊 Рекомендуемые пары",
            callback_data="action:recommended_pairs",
        )
    )
    builder.row(
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="action:back_to_menu"),
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
            callback_data=f"pair:{symbol}",
        )
    )
    builder.row(
        InlineKeyboardButton(text="📊 Рекомендуемые пары", callback_data="action:recommended_pairs"),
    )
    builder.row(
        InlineKeyboardButton(text="🏠 Главное меню", callback_data="action:back_to_menu"),
    )
    return builder.as_markup()
