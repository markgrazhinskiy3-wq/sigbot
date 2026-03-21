import logging
from datetime import date

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery

import config
from bot.keyboards import main_keyboard, cancel_keyboard
from parser.pocket_parser import parse_dashboard_stats
from utils.date_parser import get_predefined_range, parse_custom_range, fmt

logger = logging.getLogger(__name__)
router = Router()


class DateForm(StatesGroup):
    waiting_for_custom_range = State()


def _is_allowed(user_id: int) -> bool:
    if not config.ALLOWED_USER_IDS:
        return True
    return user_id in config.ALLOWED_USER_IDS


def _format_stats(date_from: date, date_to: date, stats: dict) -> str:
    period = (
        fmt(date_from)
        if date_from == date_to
        else f"{fmt(date_from)} - {fmt(date_to)}"
    )
    lines = [f"<b>Период: {period}</b>\n"]
    for key, value in stats.items():
        lines.append(f"<b>{key}:</b> {value}")
    return "\n".join(lines)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    if not _is_allowed(message.from_user.id):
        logger.warning("Unauthorized access attempt: user_id=%d", message.from_user.id)
        await message.answer("⛔ У вас нет доступа к этому боту.")
        return

    await message.answer(
        "👋 Добро пожаловать!\n\n"
        "Выберите период для получения статистики с Pocket Partners:",
        reply_markup=main_keyboard(),
    )


@router.callback_query(F.data == "cancel")
async def cb_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        "Действие отменено. Выберите период:",
        reply_markup=main_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("period:"))
async def cb_period(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_allowed(callback.from_user.id):
        await callback.answer("⛔ Нет доступа.", show_alert=True)
        return

    period = callback.data.split(":", 1)[1]

    if period == "custom":
        await state.set_state(DateForm.waiting_for_custom_range)
        await callback.message.edit_text(
            "✏️ Введите период в формате:\n"
            "<code>DD.MM.YYYY - DD.MM.YYYY</code>\n\n"
            "Например: <code>01.03.2026 - 21.03.2026</code>",
            parse_mode="HTML",
            reply_markup=cancel_keyboard(),
        )
        await callback.answer()
        return

    await callback.answer("⏳ Получаю статистику...")

    try:
        date_from, date_to = get_predefined_range(period)
    except ValueError as e:
        await callback.message.answer(f"❌ Ошибка: {e}")
        return

    await _fetch_and_reply(callback.message, date_from, date_to)


@router.message(DateForm.waiting_for_custom_range)
async def process_custom_range(message: Message, state: FSMContext) -> None:
    if not _is_allowed(message.from_user.id):
        await message.answer("⛔ Нет доступа.")
        return

    try:
        date_from, date_to = parse_custom_range(message.text.strip())
    except ValueError as e:
        await message.answer(
            f"❌ {e}\n\nПопробуйте ещё раз или нажмите Отмена.",
            reply_markup=cancel_keyboard(),
        )
        return

    await state.clear()
    await _fetch_and_reply(message, date_from, date_to)


async def _fetch_and_reply(message: Message, date_from: date, date_to: date) -> None:
    progress_msg = await message.answer("⏳ Получаю данные, подождите...")

    try:
        stats = await parse_dashboard_stats(date_from, date_to)
        text = _format_stats(date_from, date_to, stats)

        await progress_msg.edit_text(text, parse_mode="HTML", reply_markup=main_keyboard())

    except Exception as e:
        logger.exception("Failed to fetch stats: %s", e)
        await progress_msg.edit_text(
            f"❌ Не удалось получить данные.\n\n"
            f"Ошибка: <code>{str(e)[:200]}</code>\n\n"
            "Попробуйте ещё раз.",
            parse_mode="HTML",
            reply_markup=main_keyboard(),
        )
