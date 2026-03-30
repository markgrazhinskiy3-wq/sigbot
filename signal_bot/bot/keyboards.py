from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config


def _t(key: str, lang: str) -> str:
    from bot.i18n import t
    return t(key, lang)


def accept_terms_keyboard(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_t("btn_accept_terms", lang),
            callback_data="action:accept_terms",
        )
    )
    return builder.as_markup()


def lang_select_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru"),
        InlineKeyboardButton(text="🇬🇧 English", callback_data="lang:en"),
    )
    return builder.as_markup()


def main_menu_keyboard(lang: str = "ru", auto_enabled: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_t("btn_recommended", lang),
            callback_data="action:recommended_pairs",
        ),
    )
    auto_label = _t("btn_auto_on", lang) if auto_enabled else _t("btn_auto_off", lang)
    builder.row(
        InlineKeyboardButton(text=auto_label, callback_data="action:toggle_auto"),
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_change_lang", lang), callback_data="action:change_lang"),
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_restart", lang), callback_data="action:restart_bot"),
    )
    return builder.as_markup()


def pairs_keyboard(pairs: list[dict] | None = None, lang: str = "ru") -> InlineKeyboardMarkup:
    """
    Build the pair selection keyboard.
    `pairs` is a list of {"label": str, "symbol": str, "payout": int}.
    Falls back to static config.OTC_PAIRS when pairs is None.
    """
    builder = InlineKeyboardBuilder()
    source = pairs if pairs is not None else config.OTC_PAIRS
    for pair in source:
        payout = pair.get("payout", 0)
        btn_label = f"{pair['label']}  •  {payout}%" if payout else pair["label"]
        builder.row(
            InlineKeyboardButton(
                text=btn_label,
                callback_data=f"pair:{pair['symbol']}",
            )
        )
    builder.row(
        InlineKeyboardButton(text=_t("btn_refresh_list", lang), callback_data="action:refresh_pairs"),
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_back", lang), callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def expiration_keyboard(symbol: str, recommended_sec: int | None = None, lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for exp in config.EXPIRATIONS:
        builder.row(
            InlineKeyboardButton(
                text=exp["label"],
                callback_data=f"exp:{symbol}:{exp['seconds']}",
            )
        )
    builder.row(
        InlineKeyboardButton(text=_t("btn_back", lang), callback_data="action:recommended_pairs"),
    )
    return builder.as_markup()


def no_signal_keyboard(symbol: str, expiration_sec: int, lang: str = "ru") -> InlineKeyboardMarkup:
    """Keyboard shown when no signal could be generated."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_t("btn_retry", lang),
            callback_data=f"exp:{symbol}:{expiration_sec}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_t("btn_monitor_on", lang),
            callback_data=f"monitor:start:{symbol}:{expiration_sec}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_t("btn_recommended", lang),
            callback_data="action:recommended_pairs",
        )
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_other_pair", lang), callback_data="action:recommended_pairs"),
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_main_menu", lang), callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def recommended_pairs_keyboard(signals: list, lang: str = "ru") -> InlineKeyboardMarkup:
    """
    Keyboard with pairs that currently have a BUY/SELL signal.
    Each button shows pair name + payout %; clicking opens expiration picker.
    signals: list of TradabilityResult sorted by score desc.
    """
    builder = InlineKeyboardBuilder()
    for sig in signals:
        payout = getattr(sig, "payout", 0)
        btn_label = f"{sig.pair}  •  {payout}%" if payout else sig.pair
        builder.row(
            InlineKeyboardButton(
                text=btn_label,
                callback_data=f"pair:{sig.symbol}",
            )
        )
    builder.row(
        InlineKeyboardButton(text=_t("btn_refresh", lang), callback_data="action:recommended_pairs"),
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_main_menu", lang), callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def signal_result_keyboard(symbol: str, expiration_sec: int = 0, lang: str = "ru") -> InlineKeyboardMarkup:
    """Keyboard shown after a BUY/SELL signal."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_t("btn_recommended", lang),
            callback_data="action:recommended_pairs",
        )
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_main_menu", lang), callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def back_to_menu_keyboard(lang: str = "ru") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=_t("btn_main_menu", lang), callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def monitoring_active_keyboard(symbol: str, expiration_sec: int, lang: str = "ru") -> InlineKeyboardMarkup:
    """Keyboard shown while monitoring is active — lets user stop it."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_t("btn_monitor_off", lang),
            callback_data=f"monitor:stop:{symbol}:{expiration_sec}",
        )
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_main_menu", lang), callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def monitor_timeout_keyboard(lang: str = "ru") -> InlineKeyboardMarkup:
    """Keyboard shown after monitoring ends without a signal."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=_t("btn_recommended", lang), callback_data="action:recommended_pairs"),
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_main_menu", lang), callback_data="action:back_to_menu"),
    )
    return builder.as_markup()


def after_result_keyboard(symbol: str, lang: str = "ru") -> InlineKeyboardMarkup:
    """Keyboard shown after a trade result arrives."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_t("btn_next_signal", lang),
            callback_data=f"pair:{symbol}",
        )
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_recommended", lang), callback_data="action:recommended_pairs"),
    )
    builder.row(
        InlineKeyboardButton(text=_t("btn_main_menu", lang), callback_data="action:back_to_menu"),
    )
    return builder.as_markup()
