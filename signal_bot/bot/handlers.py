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
from services.access_service import notify_admin_new_user, check_access
from services.signal_service import get_signal, format_signal_message
import services.pairs_cache as pairs_cache
from bot.keyboards import (
    main_menu_keyboard, pairs_keyboard, expiration_keyboard,
    back_to_menu_keyboard, no_signal_keyboard, signal_result_keyboard,
)

logger = logging.getLogger(__name__)
router = Router()

# ── Signal pre-analysis cache ──────────────────────────────────────────────────
# {user_id: (symbol, SignalResponse)} — computed when pair is selected,
# consumed when expiration is chosen (avoids double analysis).
_signal_cache: dict[int, tuple[str, object]] = {}

_EXP_STR_TO_SEC = {"1m": 60, "2m": 120}


def _is_admin(user_id: int) -> bool:
    return user_id == config.ADMIN_USER_ID


# ─── /start ─────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user_id = message.from_user.id
    username = message.from_user.username

    user, is_new = await add_or_get_user(user_id, username)
    status = user["status"]

    if _is_admin(user_id) and status != "approved":
        await set_status(user_id, "approved")
        status = "approved"

    if is_new and status == "pending":
        await notify_admin_new_user(message.bot, user_id, username)

    if status == "pending":
        await message.answer(
            "⏳ <b>Ожидайте одобрения администратора.</b>\n\n"
            "Ваша заявка на доступ отправлена. Вы получите уведомление после рассмотрения.",
            parse_mode="HTML",
        )
        return

    if status == "denied":
        await message.answer("⛔ Доступ к боту запрещён.")
        return

    await message.answer(
        "👋 <b>Pocket Option Signal Bot</b>\n\nВыберите действие:",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


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
        "/broadcast <code>текст</code> — рассылка всем одобренным",
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
        await message.answer(
            f"✅ Пользователь <code>{target_id}</code> одобрен.", parse_mode="HTML"
        )
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
        await message.answer(
            f"❌ Пользователь <code>{target_id}</code> не найден.", parse_mode="HTML"
        )


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
        await message.answer(
            f"⛔ Пользователь <code>{target_id}</code> отклонён.", parse_mode="HTML"
        )
        try:
            await message.bot.send_message(target_id, "⛔ Ваш запрос на доступ отклонён.")
        except Exception:
            pass
    else:
        await message.answer(
            f"❌ Пользователь <code>{target_id}</code> не найден.", parse_mode="HTML"
        )


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

    await message.answer(f"📢 Рассылка: ✅ {sent} отправлено, ❌ {failed} ошибок.")


# ─── /debug — raw signal analysis dump (admin only) ──────────────────────────

@router.message(Command("debug"))
async def cmd_debug(message: Message) -> None:
    """
    /debug [PAIR_SYMBOL]
    Example: /debug #AUDCAD_otc
    Returns the full module scoring breakdown for the requested pair.
    Admin-only.
    """
    if not _is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    symbol = parts[1].strip() if len(parts) > 1 else None

    if not symbol:
        pairs = pairs_cache.get_cached()
        if not pairs:
            await message.answer("Сначала обновите кэш пар (/start → выбор пары).")
            return
        symbol = pairs[0]["symbol"]

    await message.answer(f"🔬 Запускаю анализ <code>{symbol}</code>...", parse_mode="HTML")

    try:
        pair_label = _label_for_symbol(symbol)
        signal_resp = await get_signal(symbol=symbol, pair_label=pair_label, expiration_sec=60)
    except Exception as e:
        await message.answer(f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")
        return

    details = signal_resp.details if hasattr(signal_resp, "details") else {}
    debug   = details.get("debug", {}) if isinstance(details, dict) else {}

    if not debug:
        await message.answer("Нет данных debug (не удалось получить свечи).")
        return

    # ── Format debug output ───────────────────────────────────────────────────
    lines = [
        f"🔬 <b>DEBUG: {symbol}</b>",
        f"Свечей: {debug.get('candles_count', '?')} raw → {debug.get('candles_clean', '?')} clean",
        f"Порядок: {debug.get('order', '?')} | avg_body: {float(debug.get('avg_body_pct', 0)):.5f}%",
        f"Режим рынка: {debug.get('regime', '?')}",
        f"Цена: {debug.get('last_close', '?')}",
        "",
        "<b>Модули:</b>",
    ]

    modules = debug.get("modules", {})
    module_labels = {
        "impulse_pullback": "Импульс/откат",
        "level_bounce":     "Отбой уровня",
        "false_breakout":   "Ложный пробой",
        "candle_strength":  "Сила свечей",
        "level_analysis":   "Анализ уровней",
        "market_regime":    "Режим рынка",
        "indicators":       "Индикаторы",
    }
    for key, label in module_labels.items():
        m = modules.get(key, {})
        b = m.get("buy", 0)
        s = m.get("sell", 0)
        reason = m.get("reason", "")
        lines.append(f"  {label}: BUY={b:.0f} / SELL={s:.0f}")
        if reason:
            short = reason[:80] + "…" if len(reason) > 80 else reason
            lines.append(f"    └ {short}")

    lines.append("")
    lines.append(f"PA raw buy={debug.get('pa_buy_raw','?')} sell={debug.get('pa_sell_raw','?')}")
    lines.append(f"PA blended buy={debug.get('pa_buy','?')} sell={debug.get('pa_sell','?')}")
    lines.append(f"Match: buy={'full' if debug.get('buy_full') else 'partial' if debug.get('buy_partial') else 'none'} / sell={'full' if debug.get('sell_full') else 'partial' if debug.get('sell_partial') else 'none'}")
    lines.append(f"Уверенность: {debug.get('confidence_base','?')} → {debug.get('confidence_final','?')}")
    lines.append(f"Качество: {debug.get('signal_quality', '?')}")
    lines.append(f"Экспирация: {debug.get('recommended_expiration', '?')}")
    lines.append(f"Жёсткие конфликты: {', '.join(debug.get('hard_conflicts', [])) or 'нет'}")
    lines.append(f"Мягкие штрафы: {', '.join(debug.get('soft_penalties', [])) or 'нет'}")

    final = debug.get("final_decision") or details.get("direction", "—")
    reject = debug.get("reject_reason")
    if reject:
        lines.append(f"\n❌ Отклонён: {reject}")
        if debug.get("reject_detail"):
            lines.append(f"  {debug['reject_detail']}")
    else:
        lines.append(f"\n✅ Решение: <b>{final}</b>")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3997] + "…"

    await message.answer(text, parse_mode="HTML")


# ─── Callback handlers ───────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("admin:approve:"))
async def cb_admin_approve(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user.id):
        await callback.answer("⛔ Только для администратора.", show_alert=True)
        return

    target_id = int(callback.data.split(":")[2])
    ok = await set_status(target_id, "approved")
    if ok:
        await callback.message.edit_text(
            callback.message.text + "\n\n✅ <b>Одобрен</b>",
            parse_mode="HTML",
            reply_markup=None,
        )
        try:
            await callback.bot.send_message(
                target_id,
                "✅ <b>Доступ одобрен!</b>\n\nДобро пожаловать в Signal Bot. Нажмите /start",
                parse_mode="HTML",
            )
        except Exception:
            pass
    else:
        await callback.answer("❌ Пользователь не найден.", show_alert=True)
    await callback.answer()


@router.callback_query(F.data.startswith("admin:deny:"))
async def cb_admin_deny(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user.id):
        await callback.answer("⛔ Только для администратора.", show_alert=True)
        return

    target_id = int(callback.data.split(":")[2])
    ok = await set_status(target_id, "denied")
    if ok:
        await callback.message.edit_text(
            callback.message.text + "\n\n⛔ <b>Отклонён</b>",
            parse_mode="HTML",
            reply_markup=None,
        )
        try:
            await callback.bot.send_message(
                target_id, "⛔ Ваш запрос на доступ отклонён."
            )
        except Exception:
            pass
    else:
        await callback.answer("❌ Пользователь не найден.", show_alert=True)
    await callback.answer()


async def _check_user_access(callback: CallbackQuery) -> bool:
    if _is_admin(callback.from_user.id):
        return True
    status = await check_access(callback.from_user.id)
    if status == "approved":
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


def _label_for_symbol(symbol: str) -> str:
    """
    Return the clean pair name (without payout %) for use in analysis messages.
    Checks live cache first, then config.OTC_PAIRS, then derives from symbol format.
    """
    cached = pairs_cache.get_cached()
    for p in cached:
        if p["symbol"].lower() == symbol.lower():
            return p.get("name") or p["label"].split("|")[0].strip()
    for p in config.OTC_PAIRS:
        if p["symbol"].lower() == symbol.lower():
            return p["label"]
    # Derive readable name from symbol: "#AUDCAD_otc" → "AUD/CAD OTC"
    import re as _re
    clean = _re.sub(r'[^A-Za-z]', '', symbol).upper().replace("OTC", "")
    if len(clean) == 6:
        return f"{clean[:3]}/{clean[3:]} OTC"
    return symbol


async def _show_pairs_keyboard(callback: CallbackQuery, *, force_refresh: bool = False) -> None:
    """Fetch live pairs and show the pair selection keyboard."""
    if force_refresh or not pairs_cache.is_fresh():
        await callback.message.edit_text(
            "🔄 <b>Загружаю актуальные пары с PocketOption...</b>\n\n"
            "<i>Это занимает ~20 секунд.</i>",
            parse_mode="HTML",
        )
        await pairs_cache.refresh(force=force_refresh)

    pairs = pairs_cache.get_cached()
    has_live = any(p.get("payout", 0) > 0 for p in pairs)

    if has_live:
        header = (
            f"📊 <b>Доступные OTC-пары</b>\n"
            f"<i>Только с выплатой ≥80% · Обновлено сейчас</i>"
        )
    else:
        header = (
            "📊 <b>Выберите OTC-пару:</b>\n"
            "<i>⚠️ Не удалось загрузить актуальные данные — показан стандартный список</i>"
        )

    await callback.message.edit_text(
        header,
        parse_mode="HTML",
        reply_markup=pairs_keyboard(pairs),
    )


@router.callback_query(F.data == "action:get_signal")
async def cb_get_signal(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return
    await callback.answer()
    await _show_pairs_keyboard(callback)


@router.callback_query(F.data == "action:refresh_pairs")
async def cb_refresh_pairs(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return
    await callback.answer("🔄 Обновляю список пар...")
    await _show_pairs_keyboard(callback, force_refresh=True)


@router.callback_query(F.data.startswith("pair:"))
async def cb_pair_selected(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return

    symbol = callback.data.split(":", 1)[1]
    pair_label = _label_for_symbol(symbol)
    user_id = callback.from_user.id

    await callback.answer("⏳ Анализирую пару...")
    await callback.message.edit_text(
        f"🔄 <b>Анализирую {pair_label}...</b>\n\nОпределяю оптимальное время экспирации.",
        parse_mode="HTML",
    )

    try:
        signal = await get_signal(symbol, pair_label, expiration_sec=60)
        _signal_cache[user_id] = (symbol, signal)

        d = signal.details if isinstance(signal.details, dict) else {}
        rec_str = d.get("recommended_expiration")
        rec_sec = _EXP_STR_TO_SEC.get(rec_str) if rec_str else None

        if rec_sec:
            rec_label = "1 мин" if rec_sec == 60 else "2 мин"
            hint = f"\n\n<i>💡 По текущему рынку рекомендую <b>{rec_label}</b></i>"
        else:
            hint = ""

        await callback.message.edit_text(
            f"⏱ <b>{pair_label}</b>\n\nВыберите время экспирации сделки:{hint}",
            parse_mode="HTML",
            reply_markup=expiration_keyboard(symbol, recommended_sec=rec_sec),
        )
    except Exception as e:
        logger.exception("Pre-analysis error for %s: %s", symbol, e)
        # Fallback — show picker without recommendation
        _signal_cache.pop(user_id, None)
        await callback.message.edit_text(
            f"⏱ <b>{pair_label}</b>\n\nВыберите время экспирации сделки:",
            parse_mode="HTML",
            reply_markup=expiration_keyboard(symbol),
        )


@router.callback_query(F.data.startswith("exp:"))
async def cb_expiration_selected(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return

    _, symbol, sec_str = callback.data.split(":", 2)
    expiration_sec = int(sec_str)
    pair_label = _label_for_symbol(symbol)
    user_id = callback.from_user.id

    # ── Try to use pre-analysed result from cache ──────────────────────────────
    cached = _signal_cache.get(user_id)
    if cached and cached[0] == symbol:
        from dataclasses import replace as _dc_replace
        signal = _dc_replace(cached[1], expiration_sec=expiration_sec)
        _signal_cache.pop(user_id, None)

        await callback.answer()
        text = format_signal_message(signal)
        kb = (
            no_signal_keyboard(symbol, expiration_sec)
            if signal.direction == "NO_SIGNAL"
            else signal_result_keyboard(symbol)
        )
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        return

    # ── No cache — run fresh analysis ─────────────────────────────────────────
    await callback.answer("⏳ Анализирую рынок...")
    await callback.message.edit_text(
        f"🔄 <b>Анализирую {pair_label}...</b>\n\nПодождите, собираю данные.",
        parse_mode="HTML",
    )

    try:
        signal = await get_signal(symbol, pair_label, expiration_sec)
        text = format_signal_message(signal)

        kb = (
            no_signal_keyboard(symbol, expiration_sec)
            if signal.direction == "NO_SIGNAL"
            else signal_result_keyboard(symbol)
        )
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

    except Exception as e:
        logger.exception("Signal fetch error: %s", e)
        await callback.message.edit_text(
            "❌ <b>Ошибка получения сигнала</b>\n\n"
            "Не удалось подключиться к платформе. Попробуйте позже.",
            parse_mode="HTML",
            reply_markup=back_to_menu_keyboard(),
        )


# ─── Restart bot ─────────────────────────────────────────────────────────────

@router.callback_query(F.data == "action:restart_bot")
async def cb_restart_bot(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return

    await callback.answer("🔁 Готово")
    await callback.message.edit_text(
        "✅ <b>Готово.</b>\n\nМожно запрашивать новые сигналы.",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )
