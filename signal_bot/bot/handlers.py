import logging

from aiogram import Router, F, Bot
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
from db.database import (
    add_or_get_user, get_status, set_status,
    list_all, list_pending, list_approved,
)
from services.access_service import check_access
from services.signal_service import get_signal, format_signal_message
from services.result_watcher import schedule_result_watcher
from bot.keyboards import (
    main_menu_keyboard, pairs_keyboard, expiration_keyboard, back_to_menu_keyboard
)

logger = logging.getLogger(__name__)
router = Router()


def _is_admin(user_id: int) -> bool:
    return user_id == config.ADMIN_USER_ID


# ─── /start ─────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user_id = message.from_user.id
    username = message.from_user.username

    user = await add_or_get_user(user_id, username)
    status = user["status"]

    if status == "pending":
        await message.answer(
            "⏳ <b>Ожидайте одобрения администратора.</b>\n\n"
            "Ваша заявка на доступ отправлена. Вы получите уведомление после рассмотрения.",
            parse_mode="HTML",
        )
        await _notify_admin_new_user(message.bot, user_id, username)
        return

    if status == "denied":
        await message.answer(
            "⛔ Доступ к боту запрещён.",
            parse_mode="HTML",
        )
        return

    if _is_admin(user_id) and status != "approved":
        await set_status(user_id, "approved")

    await message.answer(
        "👋 <b>Pocket Option Signal Bot</b>\n\n"
        "Выберите действие:",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


async def _notify_admin_new_user(bot: Bot, user_id: int, username: str | None) -> None:
    uname = f"@{username}" if username else f"ID:{user_id}"
    try:
        await bot.send_message(
            config.ADMIN_USER_ID,
            f"🆕 <b>Новый запрос на доступ</b>\n\n"
            f"Пользователь: {uname}\n"
            f"ID: <code>{user_id}</code>\n\n"
            f"✅ Одобрить: /approve {user_id}\n"
            f"❌ Отклонить: /deny {user_id}",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("Failed to notify admin: %s", e)


# ─── Admin commands ──────────────────────────────────────────────────────────

@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if not _is_admin(message.from_user.id):
        return

    await message.answer(
        "🔧 <b>Панель администратора</b>\n\n"
        "Команды:\n"
        "/approve <code>ID</code> — одобрить пользователя\n"
        "/deny <code>ID</code> — отклонить пользователя\n"
        "/users — список всех пользователей\n"
        "/pending — список ожидающих\n"
        "/broadcast <code>текст</code> — рассылка всем",
        parse_mode="HTML",
    )


@router.message(Command("approve"))
async def cmd_approve(message: Message) -> None:
    if not _is_admin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.answer("Использование: /approve <user_id>")
        return

    target_id = int(parts[1])
    ok = await set_status(target_id, "approved")
    if ok:
        await message.answer(f"✅ Пользователь <code>{target_id}</code> одобрен.", parse_mode="HTML")
        try:
            await message.bot.send_message(
                target_id,
                "✅ <b>Доступ одобрен!</b>\n\nДобро пожаловать в Signal Bot.",
                parse_mode="HTML",
                reply_markup=main_menu_keyboard(),
            )
        except Exception:
            pass
    else:
        await message.answer(f"❌ Пользователь <code>{target_id}</code> не найден.", parse_mode="HTML")


@router.message(Command("deny"))
async def cmd_deny(message: Message) -> None:
    if not _is_admin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.answer("Использование: /deny <user_id>")
        return

    target_id = int(parts[1])
    ok = await set_status(target_id, "denied")
    if ok:
        await message.answer(f"⛔ Пользователь <code>{target_id}</code> отклонён.", parse_mode="HTML")
        try:
            await message.bot.send_message(
                target_id,
                "⛔ Ваш запрос на доступ отклонён.",
            )
        except Exception:
            pass
    else:
        await message.answer(f"❌ Пользователь <code>{target_id}</code> не найден.", parse_mode="HTML")


@router.message(Command("users"))
async def cmd_users(message: Message) -> None:
    if not _is_admin(message.from_user.id):
        return

    users = await list_all()
    if not users:
        await message.answer("Список пользователей пуст.")
        return

    lines = ["<b>Все пользователи:</b>\n"]
    for u in users:
        emoji = {"approved": "✅", "pending": "⏳", "denied": "⛔"}.get(u["status"], "❓")
        uname = f"@{u['username']}" if u.get("username") else "—"
        lines.append(f"{emoji} <code>{u['user_id']}</code> {uname} [{u['status']}]")

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("pending"))
async def cmd_pending(message: Message) -> None:
    if not _is_admin(message.from_user.id):
        return

    users = await list_pending()
    if not users:
        await message.answer("⏳ Нет ожидающих пользователей.")
        return

    lines = ["<b>Ожидают одобрения:</b>\n"]
    for u in users:
        uname = f"@{u['username']}" if u.get("username") else "—"
        lines.append(
            f"⏳ <code>{u['user_id']}</code> {uname}\n"
            f"  /approve {u['user_id']}  |  /deny {u['user_id']}"
        )

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message) -> None:
    if not _is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /broadcast <текст>")
        return

    text = parts[1]
    users = await list_approved()

    sent, failed = 0, 0
    for u in users:
        try:
            await message.bot.send_message(u["user_id"], text)
            sent += 1
        except Exception:
            failed += 1

    await message.answer(f"📢 Рассылка завершена: ✅ {sent} отправлено, ❌ {failed} ошибок.")


# ─── Callback handlers ───────────────────────────────────────────────────────

async def _check_user_access(callback: CallbackQuery) -> bool:
    status = await check_access(callback.from_user.id)
    if status == "approved" or _is_admin(callback.from_user.id):
        return True
    if status == "pending":
        await callback.answer("⏳ Ваша заявка ещё на рассмотрении.", show_alert=True)
    elif status == "denied":
        await callback.answer("⛔ Доступ запрещён.", show_alert=True)
    else:
        await callback.answer("⛔ Нет доступа. Напишите /start", show_alert=True)
    return False


@router.callback_query(F.data == "action:back_to_menu")
async def cb_back_to_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        "👋 <b>Pocket Option Signal Bot</b>\n\nВыберите действие:",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data == "action:get_signal")
async def cb_get_signal(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return

    await callback.message.edit_text(
        "📊 Выберите OTC-пару:",
        reply_markup=pairs_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("pair:"))
async def cb_pair_selected(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return

    symbol = callback.data.split(":", 1)[1]
    pair_label = next(
        (p["label"] for p in config.OTC_PAIRS if p["symbol"] == symbol),
        symbol,
    )

    await callback.message.edit_text(
        f"⏱ <b>{pair_label}</b>\n\nВыберите время экспирации:",
        parse_mode="HTML",
        reply_markup=expiration_keyboard(symbol),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("exp:"))
async def cb_expiration_selected(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return

    _, symbol, sec_str = callback.data.split(":", 2)
    expiration_sec = int(sec_str)
    pair_label = next(
        (p["label"] for p in config.OTC_PAIRS if p["symbol"] == symbol),
        symbol,
    )

    await callback.answer("⏳ Анализирую рынок...")
    await callback.message.edit_text(
        f"🔄 <b>Анализирую {pair_label}...</b>\n\nПодождите, собираю данные.",
        parse_mode="HTML",
    )

    try:
        signal = await get_signal(symbol, pair_label, expiration_sec)
        text = format_signal_message(signal)

        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=back_to_menu_keyboard(),
        )

        if signal.direction != "NO_SIGNAL":
            schedule_result_watcher(
                bot=callback.bot,
                chat_id=callback.from_user.id,
                symbol=symbol,
                pair_label=pair_label,
                expiration_sec=expiration_sec,
                direction=signal.direction,
            )

    except Exception as e:
        logger.exception("Signal fetch error: %s", e)
        await callback.message.edit_text(
            f"❌ <b>Ошибка получения сигнала</b>\n\n"
            f"<code>{str(e)[:300]}</code>",
            parse_mode="HTML",
            reply_markup=back_to_menu_keyboard(),
        )
