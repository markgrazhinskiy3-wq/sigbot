import asyncio
import logging

from aiogram import Router, F, Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, CallbackQuery

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
from db.database import (
    add_or_get_user, get_status, set_status,
    list_all, list_pending, list_approved,
    save_signal_outcome, get_user_stats, get_strategy_stats,
)
from services.access_service import notify_admin_new_user, check_access
from services.signal_service import get_signal, scan_all_signals, format_signal_message
from services.candle_cache import is_warm_up_done
from services.outcome_tracker import track_outcome
import services.pairs_cache as pairs_cache
from bot.keyboards import (
    main_menu_keyboard, pairs_keyboard, expiration_keyboard,
    back_to_menu_keyboard, no_signal_keyboard, signal_result_keyboard,
    recommended_pairs_keyboard,
)

logger = logging.getLogger(__name__)
router = Router()

# Pending signal info per user — populated when signal is shown, consumed when user clicks
# "🟢 Открываю сделку". Keyed by user_id.
_pending_signals: dict[int, dict] = {}


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

    await callback.message.edit_text(
        f"⏱ <b>{pair_label}</b>\n\nВыберите время экспирации сделки:",
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
    pair_label = _label_for_symbol(symbol)

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
            else signal_result_keyboard(symbol, expiration_sec)
        )
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)

        # ── Store pending signal for "🟢 Открываю сделку" button ──────────────
        # Track_outcome starts ONLY when user confirms they placed the trade —
        # this ensures the timer and entry price are accurate.
        if signal.direction in ("BUY", "SELL"):
            d = signal.details if isinstance(signal.details, dict) else {}
            strategy = d.get("primary_strategy")
            _pending_signals[callback.from_user.id] = {
                "symbol":         symbol,
                "pair_label":     pair_label,
                "direction":      signal.direction,
                "confidence":     signal.confidence,
                "strategy":       strategy,
                "expiration_sec": expiration_sec,
            }

    except Exception as e:
        logger.exception("Signal fetch error: %s", e)
        await callback.message.edit_text(
            "❌ <b>Ошибка получения сигнала</b>\n\n"
            "Не удалось подключиться к платформе. Попробуйте позже.",
            parse_mode="HTML",
            reply_markup=back_to_menu_keyboard(),
        )


# ─── Start trade (outcome timer) ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("start_trade:"))
async def cb_start_trade(callback: CallbackQuery) -> None:
    """
    User clicked "🟢 Открываю сделку" — THIS is the true trade start moment.
    We grab the current price from cache as the entry price, then start the
    outcome tracker with a timer beginning NOW (not at signal generation).
    """
    if not await _check_user_access(callback):
        return

    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await callback.answer("Ошибка данных кнопки.")
        return

    _, symbol, sec_str = parts
    expiration_sec = int(sec_str)

    pending = _pending_signals.pop(callback.from_user.id, None)
    if pending is None:
        await callback.answer("Сигнал уже устарел — запросите новый.", show_alert=True)
        return

    pair_label = pending["pair_label"]
    direction  = pending["direction"]
    strategy   = pending.get("strategy")
    confidence = pending.get("confidence", 0)

    from services.candle_cache import get_cached
    candles = get_cached(symbol)
    if not candles:
        await callback.answer("Нет данных о цене — запросите сигнал снова.", show_alert=True)
        return

    entry_price = float(candles[-1]["close"])

    try:
        outcome_id = await save_signal_outcome(
            user_id        = callback.from_user.id,
            symbol         = symbol,
            pair_label     = pair_label,
            direction      = direction,
            confidence     = confidence,
            strategy       = strategy,
            expiration_sec = expiration_sec,
            signal_price   = entry_price,
        )
        asyncio.create_task(track_outcome(
            bot            = callback.bot,
            chat_id        = callback.from_user.id,
            outcome_id     = outcome_id,
            symbol         = symbol,
            pair_label     = pair_label,
            direction      = direction,
            strategy       = strategy,
            expiration_sec = expiration_sec,
            signal_price   = entry_price,
        ))
    except Exception as e:
        logger.exception("Failed to save/start outcome tracking: %s", e)

    exp_label = f"{expiration_sec // 60} мин" if expiration_sec >= 60 else f"{expiration_sec} сек"
    dir_text  = "ВВЕРХ ⬆️" if direction == "BUY" else "ВНИЗ ⬇️"

    await callback.message.edit_text(
        f"⏳ <b>Сделка открыта!</b>\n\n"
        f"📊 {pair_label} · {dir_text} · {exp_label}\n"
        f"Цена входа: <code>{entry_price:.6g}</code>\n\n"
        f"<i>Результат придёт через {expiration_sec} сек...</i>",
        parse_mode="HTML",
    )
    await callback.answer("Таймер запущен!")


# ─── Recommended pairs ───────────────────────────────────────────────────────

def _build_pairs_map() -> dict[str, str]:
    pairs_map = {p["symbol"]: p.get("name") or p["label"].split("|")[0].strip()
                 for p in pairs_cache.get_cached()}
    for p in config.OTC_PAIRS:
        if p["symbol"] not in pairs_map:
            pairs_map[p["symbol"]] = p["label"]
    return pairs_map


@router.callback_query(F.data == "action:recommended_pairs")
async def cb_recommended_pairs(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return

    await callback.answer("📊 Сканирую пары...")
    await callback.message.edit_text(
        "📊 <b>Сканирую пары...</b>\n\nАнализирую рынок, подождите секунду.",
        parse_mode="HTML",
    )

    try:
        pairs_map = _build_pairs_map()
        signals = await scan_all_signals(pairs_map)

        if not signals:
            if not is_warm_up_done():
                await callback.message.edit_text(
                    "⏳ <b>Бот загружается...</b>\n\n"
                    "Идёт начальный прогрев данных (~2–3 мин после запуска).\n\n"
                    "<i>Подождите немного и нажмите «Обновить».</i>",
                    parse_mode="HTML",
                    reply_markup=recommended_pairs_keyboard([]),
                )
            else:
                await callback.message.edit_text(
                    "⚠️ <b>Рекомендуемых пар нет</b>\n\n"
                    "Сейчас на всех парах нет чёткого сигнала — рынок неопределённый.\n\n"
                    "<i>Подождите 1–2 минуты и нажмите «Обновить».</i>",
                    parse_mode="HTML",
                    reply_markup=recommended_pairs_keyboard([]),
                )
            return

        lines = [
            "📊 <b>Пары с активными сигналами:</b>\n",
        ]
        for sig in signals:
            lines.append(f"• {sig.pair}")

        lines.append("\n<i>Переключитесь на нужную пару в Pocket Option, затем нажмите на неё ниже — сигнал будет рассчитан для выбранного времени экспирации.</i>")

        await callback.message.edit_text(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=recommended_pairs_keyboard(signals),
        )

    except Exception as e:
        logger.exception("recommended_pairs error: %s", e)
        await callback.message.edit_text(
            "❌ <b>Ошибка сканирования</b>\n\nПопробуйте позже.",
            parse_mode="HTML",
            reply_markup=back_to_menu_keyboard(),
        )


# ─── Restart bot ─────────────────────────────────────────────────────────────

@router.callback_query(F.data == "action:restart_bot")
async def cb_restart_bot(callback: CallbackQuery) -> None:
    if not await _check_user_access(callback):
        return

    await callback.answer("🔁 Готово")
    try:
        await callback.message.edit_text(
            "✅ <b>Готово.</b>\n\nМожно запрашивать новые сигналы.",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )
    except TelegramBadRequest:
        pass


# ─── /stats ──────────────────────────────────────────────────────────────────

@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    user_id = message.from_user.id
    status = await get_status(user_id)
    if status != "approved":
        await message.answer("❌ У вас нет доступа к боту.")
        return

    stats    = await get_user_stats(user_id)
    by_strat = await get_strategy_stats(user_id)

    total   = stats["total"]
    wins    = stats["wins"]
    losses  = stats["losses"]
    pending = stats["pending"]
    winrate = stats["winrate"]

    if total == 0:
        await message.answer(
            "📊 <b>Ваша статистика</b>\n\n"
            "Пока нет завершённых сигналов.\n"
            "Запросите первый сигнал — результат появится здесь автоматически.",
            parse_mode="HTML",
        )
        return

    wr_str = f"{winrate}%" if winrate is not None else "—"

    lines = [
        "📊 <b>Ваша статистика</b>\n",
        f"Всего сигналов:  <b>{total}</b>",
        f"✅ Прибыльных:   <b>{wins}</b>",
        f"❌ Убыточных:    <b>{losses}</b>",
        f"⏳ В процессе:   <b>{pending}</b>",
        f"🎯 Точность:     <b>{wr_str}</b>",
    ]

    if by_strat:
        _strat_names = {
            "impulse":  "Импульс",
            "bounce":   "Отскок",
            "breakout": "Лож. пробой",
        }
        lines.append("\n<b>По стратегиям:</b>")
        for s in by_strat:
            name = _strat_names.get(s["strategy"], s["strategy"])
            wr   = f"{s['winrate']}%" if s["winrate"] is not None else "—"
            lines.append(f"  {name}: {s['wins']}W / {s['losses']}L  ({wr})")

    await message.answer("\n".join(lines), parse_mode="HTML")
