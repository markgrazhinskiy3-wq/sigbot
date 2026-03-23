import asyncio
import html
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
    save_signal_outcome, get_user_stats, get_strategy_stats, get_pair_stats,
    get_daily_admin_stats, get_performance_report,
    get_all_admin_ids, get_all_admins, add_admin, remove_admin,
    get_condition_stats, reset_condition_stats,
    get_pair_outcomes,
)
from services.access_service import notify_admin_new_user, check_access
from services.signal_service import get_signal, format_signal_message
from services.candle_cache import is_warm_up_done, is_data_ready, data_ready_in_seconds
from services.outcome_tracker import track_outcome
from services.analysis.asset_scanner import (
    scan_all_pairs, scan_pairs_fresh, format_scan_output, get_scan_cache, TradabilityResult,
)
import services.pairs_cache as pairs_cache

# ── Active monitoring tasks: {user_id: asyncio.Task} ─────────────────────────
_monitor_tasks: dict[int, asyncio.Task] = {}

# ── Last selected pair per user (for /debug without args) ─────────────────────
_last_pair: dict[int, str] = {}
from bot.keyboards import (
    main_menu_keyboard, pairs_keyboard, expiration_keyboard,
    back_to_menu_keyboard, no_signal_keyboard, signal_result_keyboard,
    recommended_pairs_keyboard,
)

logger = logging.getLogger(__name__)
router = Router()

# ── Admin cache ────────────────────────────────────────────────────────────────
# In-memory set of assigned admin IDs (loaded at startup, updated on add/remove).
# Main admin (config.ADMIN_USER_ID) is always admin regardless of this set.
_extra_admin_ids: set[int] = set()


async def load_admin_cache() -> None:
    """Load assigned admin IDs from DB into memory. Call once at startup."""
    global _extra_admin_ids
    _extra_admin_ids = await get_all_admin_ids()
    logger.info("Admin cache loaded: %d extra admin(s)", len(_extra_admin_ids))


def _is_main_admin(user_id: int) -> bool:
    """True only for the primary admin (configured via ADMIN_USER_ID env)."""
    return user_id == config.ADMIN_USER_ID


def _is_admin(user_id: int) -> bool:
    """True for main admin OR any assigned admin."""
    return user_id == config.ADMIN_USER_ID or user_id in _extra_admin_ids


def _cancel_monitor(user_id: int) -> None:
    """Cancel any active monitoring task for the user."""
    task = _monitor_tasks.pop(user_id, None)
    if task and not task.done():
        task.cancel()


def _confidence_band(conf_raw: float | None) -> str:
    """Map raw confidence (0-100) to a readable band label."""
    if conf_raw is None:
        return "unknown"
    if conf_raw >= 88:
        return "88+"
    if conf_raw >= 80:
        return "80-87"
    if conf_raw >= 70:
        return "70-79"
    if conf_raw >= 58:
        return "58-69"
    return "52-57"


def _extract_signal_meta(details: dict) -> dict:
    """Extract performance-tracking metadata from signal details dict."""
    conf_raw = details.get("confidence_raw")
    return {
        "market_mode":     details.get("market_mode"),
        "used_tier":       details.get("debug", {}).get("used_tier"),
        "confidence_raw":  conf_raw,
        "confidence_band": _confidence_band(conf_raw),
    }


async def _monitor_pair(
    bot: Bot,
    chat_id: int,
    symbol: str,
    pair_label: str,
    expiration_sec: int,
) -> None:
    """
    Background task: persistent WS connection to PocketOption, checks for a
    signal on every new candle (~15 sec interval). One connection, no reconnects.
    Stops when signal fires, conditions worsen, or 5 minutes elapsed.
    """
    from services.analysis.asset_scanner import calculate_tradability
    from services.strategy_engine import calculate_signal, SignalResult
    from services.signal_service import SignalResponse, format_signal_message as _fmt
    from services.po_ws_client import stream_pair
    from bot.keyboards import signal_result_keyboard, back_to_menu_keyboard, monitor_timeout_keyboard

    checks = 0
    signal_found = False
    conditions_worsened = False

    async def on_candles(candles: list) -> bool:
        nonlocal checks, signal_found, conditions_worsened
        checks += 1

        if len(candles) < 20:
            return False

        # Check if conditions have worsened
        tr = calculate_tradability(symbol, pair_label, candles)
        if tr and tr.score < 40:
            conditions_worsened = True
            await bot.send_message(
                chat_id,
                f"⚠️ <b>Условия на {pair_label} ухудшились</b>\n\n"
                f"Скор торгуемости упал до {tr.score}/100.\n"
                f"Рекомендуем выбрать другую пару.",
                parse_mode="HTML",
                reply_markup=monitor_timeout_keyboard(),
            )
            return True  # stop streaming

        # Run signal engine directly on fresh candles (no extra WS connection)
        try:
            result: SignalResult = await calculate_signal(candles)
        except Exception as exc:
            logger.debug("Monitor signal calc failed (check %d): %s", checks, exc)
            return False

        if result.direction not in ("BUY", "SELL"):
            logger.debug(
                "Monitor check %d for %s: NO_SIGNAL (confidence=%s)",
                checks, symbol, result.confidence,
            )
            return False

        # Build a synthetic SignalResponse for formatting
        from services.signal_service import _expiry_seconds
        exp = _expiry_seconds(result.details) if expiration_sec == 0 else expiration_sec
        signal = SignalResponse(
            direction=result.direction,
            confidence=result.confidence,
            details=result.details,
            pair=pair_label,
            expiration_sec=exp,
            symbol=symbol,
        )

        text = _fmt(signal)
        await bot.send_message(
            chat_id, text, parse_mode="HTML",
            reply_markup=signal_result_keyboard(symbol, exp),
        )

        d            = result.details if isinstance(result.details, dict) else {}
        signal_price = d.get("debug", {}).get("last_close")
        strategy     = d.get("primary_strategy")
        if signal_price:
            meta = _extract_signal_meta(d)
            outcome_id = await save_signal_outcome(
                user_id=chat_id, symbol=symbol, pair_label=pair_label,
                direction=signal.direction, confidence=signal.confidence,
                strategy=strategy, expiration_sec=exp,
                signal_price=signal_price,
                **meta,
            )
            asyncio.create_task(track_outcome(
                bot=bot, chat_id=chat_id, outcome_id=outcome_id,
                symbol=symbol, pair_label=pair_label,
                direction=signal.direction, strategy=strategy,
                expiration_sec=exp, signal_price=signal_price,
            ))

        signal_found = True
        return True  # stop streaming

    cancelled = False
    try:
        await stream_pair(
            symbol=symbol,
            on_candles=on_candles,
            max_duration=300.0,
            check_interval=15.0,
        )
    except asyncio.CancelledError:
        cancelled = True
    except Exception as exc:
        logger.warning("Monitor task error for %s: %s", symbol, exc)
        cancelled = True
    finally:
        _monitor_tasks.pop(chat_id, None)

    # Notify user only on natural timeout (not cancelled by user, no signal, no bad conditions)
    if not cancelled and not signal_found and not conditions_worsened:
        try:
            await bot.send_message(
                chat_id,
                f"⏱ <b>Мониторинг завершён — {pair_label}</b>\n\n"
                f"За 5 минут сигнал не появился.\n"
                f"Попробуйте другую пару или запустите мониторинг снова.",
                parse_mode="HTML",
                reply_markup=monitor_timeout_keyboard(),
            )
        except Exception as exc:
            logger.debug("Timeout notification failed: %s", exc)


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
    raw_symbol = parts[1].strip() if len(parts) > 1 else None

    if raw_symbol:
        symbol = raw_symbol
        _last_pair[message.from_user.id] = symbol   # update memory
    else:
        symbol = _last_pair.get(message.from_user.id)
        if not symbol:
            pairs = pairs_cache.get_cached()
            if not pairs:
                await message.answer("Сначала выберите пару через меню, чтобы /debug запомнил её.")
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

    direction = details.get("direction", "NO_SIGNAL")
    last_close = debug.get("last_close", "?")

    # ── Bar counts ──────────────────────────────────────────────────────────
    n15  = debug.get("n_bars_15s") or debug.get("candles_raw", "?")
    n1m  = debug.get("n_bars_1m", 0)
    n5m  = debug.get("n_bars_5m", 0)
    nc   = debug.get("candles_clean", "?")
    abp  = debug.get("avg_body_pct", 0)

    lines = [
        f"🔬 <b>DEBUG {symbol}</b>",
        f"Цена: <code>{last_close}</code> | Направление: <b>{direction}</b>",
        "",
        f"📊 <b>Свечи</b>",
        f"  15s bars: <b>{n15}</b> raw → <b>{nc}</b> clean",
        f"  1m bars:  <b>{n1m}</b> | 5m bars: <b>{n5m}</b>",
        f"  avg_body: {float(abp):.5f}%",
    ]

    # ── Market mode ─────────────────────────────────────────────────────────
    mode     = debug.get("mode") or details.get("market_mode", "?")
    mode_str = debug.get("mode_strength", "?")
    mode_exp = debug.get("mode_explanation", "")
    mode_dbg = debug.get("mode_debug") or debug.get("mode_debug", {})
    lines += [
        "",
        f"🧭 <b>Режим рынка: {mode}</b> (сила={mode_str})",
    ]
    if mode_exp:
        lines.append(f"  {html.escape(str(mode_exp))}")
    if isinstance(mode_dbg, dict) and mode_dbg:
        parts = [f"{html.escape(str(k))}={html.escape(str(v))}" for k, v in mode_dbg.items()]
        lines.append(f"  {' | '.join(parts)}")

    # ── Indicators ──────────────────────────────────────────────────────────
    ind = debug.get("indicators", {})
    if ind:
        atr       = ind.get("atr",       "?")
        atr_ratio = ind.get("atr_ratio", "?")
        rsi       = ind.get("rsi",       "?")
        sk        = ind.get("stoch_k",   "?")
        sd        = ind.get("stoch_d",   "?")
        e5        = ind.get("ema5",      "?")
        e13       = ind.get("ema13",     "?")
        e21       = ind.get("ema21",     "?")
        bw        = ind.get("bb_bw",     "?")
        mom       = ind.get("momentum",  "?")
        # EMA spread vs price
        if isinstance(e5, float) and isinstance(e21, float) and isinstance(last_close, float) and last_close:
            spread_pct = (e5 - e21) / last_close * 100
            spread_str = f"{spread_pct:+.4f}%"
        else:
            spread_str = "?"
        lines += [
            "",
            "📉 <b>Индикаторы (15s bars)</b>",
            f"  ATR:      {atr}  (×{atr_ratio} avg)",
            f"  RSI(7):   {rsi}",
            f"  Stoch:    K={sk}  D={sd}",
            f"  EMA5:     {e5}",
            f"  EMA13:    {e13}",
            f"  EMA21:    {e21}",
            f"  EMA5-21:  {spread_str}",
            f"  BB_bw:    {bw}",
            f"  Momentum: {mom}",
        ]

    # ── Levels ──────────────────────────────────────────────────────────────
    lvl = debug.get("levels", {})
    if lvl:
        lines += [
            "",
            "📌 <b>Уровни</b>",
            f"  Ближ. суп: {lvl.get('nearest_sup','?')}  ({lvl.get('dist_sup_pct','?')}% от цены)",
            f"  Ближ. рез: {lvl.get('nearest_res','?')}  ({lvl.get('dist_res_pct','?')}% от цены)",
            f"  Кол-во:    {lvl.get('n_supports',0)} суп  /  {lvl.get('n_resistances',0)} рез",
        ]

    # ── Context ─────────────────────────────────────────────────────────────
    cup  = debug.get("ctx_up_1m",    debug.get("ctx_up",   False))
    cdn  = debug.get("ctx_dn_1m",    debug.get("ctx_down", False))
    mnot = debug.get("ctx_macro_note", "—")
    lines += [
        "",
        "🔗 <b>Контекст MTF</b>",
        f"  1m EMA:   {'↑' if cup else '↓' if cdn else '—'}",
        f"  Macro:    {html.escape(str(mnot))}",
    ]

    # ── Per-strategy breakdown ───────────────────────────────────────────────
    strategies = debug.get("strategies", {})
    used_tier  = debug.get("used_tier", "?")
    if strategies:
        lines += ["", f"🎯 <b>Стратегии</b> (tier запущен: {used_tier})"]
        _TIER_ICONS = {"primary": "①", "secondary": "②"}
        for sname, sd in strategies.items():
            if sd.get("skipped"):
                lines.append(f"  {sname}: ⛔ DISABLED")
                continue
            sdir  = sd.get("direction", "NO_SIGNAL")
            sconf = sd.get("confidence", 0)
            scm   = sd.get("conditions_met", 0)
            stot  = sd.get("total", 0)
            spct  = sd.get("pct", 0)
            stier = sd.get("tier", "?")
            icon  = _TIER_ICONS.get(stier, "?")
            fired = "✅" if sdir in ("BUY", "SELL") else "❌"
            mult  = sd.get("adaptation_multiplier", 1.0)
            mult_str = f" ×{mult:.2f}" if mult != 1.0 else ""
            early = sd.get("early_reject")
            lines.append(
                f"  {icon} <b>{sname}</b>: {fired} {sdir} | {scm}/{stot} ({spct}%) | conf={sconf}{mult_str}"
            )
            if early:
                lines.append(f"    ⛔ Ранний отказ: {html.escape(str(early))}")
            # Per-condition breakdown
            conds = sd.get("conditions", {})
            if conds:
                for cname, cval in conds.items():
                    if isinstance(cval, bool):
                        mark = "✅" if cval else "❌"
                        lines.append(f"    {mark} {cname}")
                    elif isinstance(cval, str):
                        # e.g. pattern_type = "pin_bar" — show as info
                        lines.append(f"    ℹ️ {cname}: {html.escape(cval)}")

    # ── Final decision ───────────────────────────────────────────────────────
    lines.append("")
    if direction in ("BUY", "SELL"):
        conf_raw  = details.get("confidence_raw", debug.get("conf_after_multipliers", "?"))
        conf_5    = details.get("confidence_5", signal_resp.confidence)
        quality   = details.get("signal_quality", "?")
        expiry    = details.get("expiry_hint", "?")
        strat     = details.get("primary_strategy", "?")
        tier_used = debug.get("used_tier", "?")
        lines += [
            f"✅ <b>Сигнал: {direction}</b>",
            f"  Стратегия: {strat} [{tier_used}]",
            f"  conf_raw={conf_raw}  ⭐{conf_5}  ({quality})",
            f"  Экспирация: {expiry}",
        ]
        reasoning = details.get("reasoning", "")
        if reasoning:
            lines.append(f"  Причина: {html.escape(str(reasoning)[:120])}")
    else:
        reject = details.get("reject_reason") or details.get("reasoning", "—")
        conf_r = debug.get("conf_raw")
        thresh = debug.get("min_threshold")
        lines.append(f"❌ <b>NO_SIGNAL</b>")
        lines.append(f"  Причина: {html.escape(str(reject))}")
        if conf_r is not None and thresh is not None:
            lines.append(f"  conf={conf_r} &lt; порог={thresh}")

    # ── Trade history from DB ─────────────────────────────────────────────────
    history = await get_pair_outcomes(symbol, limit=15)
    if history:
        wins   = sum(1 for r in history if r["outcome"] == "win")
        losses = sum(1 for r in history if r["outcome"] == "loss")
        total  = wins + losses
        wr     = round(wins / total * 100) if total else 0
        lines += [
            "",
            f"📂 <b>История сделок ({total}): {wins}W / {losses}L — {wr}%</b>",
        ]
        for r in history:
            icon   = "✅" if r["outcome"] == "win" else "❌"
            ts     = (r["created_at"] or "")[:16].replace("T", " ")
            strat  = (r["strategy"] or "?")
            mode   = (r["market_mode"] or "?")
            conf_r = r.get("confidence_raw")
            conf_str = f" conf={conf_r:.0f}" if conf_r else ""
            exp_sec = r.get("expiration_sec") or 0
            exp_str = f"{exp_sec//60}m" if exp_sec >= 60 else f"{exp_sec}s"
            entry  = r.get("signal_price") or 0
            result = r.get("result_price") or 0
            diff   = (result - entry) / entry * 100 if entry else 0
            diff_str = f"{diff:+.3f}%"
            lines.append(
                f"  {icon} {ts} | {r['direction']} {exp_str}{conf_str} | {strat}/{mode} | {diff_str}"
            )
    else:
        lines += ["", "📂 <b>История:</b> сделок по этой паре ещё нет"]

    text = "\n".join(lines)
    # Telegram limit: split if needed
    LIMIT = 4000
    if len(text) <= LIMIT:
        await message.answer(text, parse_mode="HTML")
    else:
        chunk, length = [], 0
        for line in lines:
            length += len(line) + 1
            if length > LIMIT:
                await message.answer("\n".join(chunk), parse_mode="HTML")
                chunk, length = [line], len(line) + 1
            else:
                chunk.append(line)
        if chunk:
            await message.answer("\n".join(chunk), parse_mode="HTML")


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
    _cancel_monitor(callback.from_user.id)
    await state.clear()
    await callback.message.edit_text(
        "👋 <b>Pocket Option Signal Bot</b>\n\nВыберите действие:",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("monitor:start:"))
async def cb_monitor_start(callback: CallbackQuery) -> None:
    """User manually pressed 'Enable monitoring' button."""
    from bot.keyboards import monitoring_active_keyboard
    if not await _check_user_access(callback):
        return

    parts = callback.data.split(":", 3)
    symbol = parts[2]
    expiration_sec = int(parts[3])
    pair_label = _label_for_symbol(symbol)
    user_id = callback.from_user.id

    _cancel_monitor(user_id)
    task = asyncio.create_task(
        _monitor_pair(
            bot=callback.bot,
            chat_id=user_id,
            symbol=symbol,
            pair_label=pair_label,
            expiration_sec=expiration_sec,
        )
    )
    _monitor_tasks[user_id] = task

    await callback.message.edit_text(
        f"🔔 <b>Мониторинг запущен — {pair_label}</b>\n\n"
        f"Бот отслеживает пару в реальном времени (до 5 минут).\n"
        f"Как только появится сигнал — пришлёт уведомление автоматически.\n\n"
        f"<i>Нажмите «Остановить», чтобы отменить мониторинг.</i>",
        parse_mode="HTML",
        reply_markup=monitoring_active_keyboard(symbol, expiration_sec),
    )
    await callback.answer("🔔 Мониторинг запущен")


@router.callback_query(F.data.startswith("monitor:stop:"))
async def cb_monitor_stop(callback: CallbackQuery) -> None:
    """User manually pressed 'Stop monitoring' button."""
    from bot.keyboards import no_signal_keyboard
    if not await _check_user_access(callback):
        return

    parts = callback.data.split(":", 3)
    symbol = parts[2]
    expiration_sec = int(parts[3])
    pair_label = _label_for_symbol(symbol)
    user_id = callback.from_user.id

    _cancel_monitor(user_id)

    await callback.message.edit_text(
        f"⏹ <b>Мониторинг остановлен — {pair_label}</b>\n\n"
        f"Вы можете запросить сигнал вручную или выбрать другую пару.",
        parse_mode="HTML",
        reply_markup=no_signal_keyboard(symbol, expiration_sec),
    )
    await callback.answer("Мониторинг остановлен")


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

    all_pairs = pairs_cache.get_cached()
    has_live = any(p.get("payout", 0) > 0 for p in all_pairs)

    # For the manual pair list, show only pairs with payout >= 85%
    SIGNAL_MIN_PAYOUT = 85
    if has_live:
        pairs = [p for p in all_pairs if p.get("payout", 0) >= SIGNAL_MIN_PAYOUT or p.get("payout", 0) == 0]
        header = (
            f"📊 <b>Доступные OTC-пары</b>\n"
            f"<i>Только с выплатой ≥{SIGNAL_MIN_PAYOUT}% · Обновлено сейчас</i>"
        )
    else:
        pairs = all_pairs
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

    # Remember this pair for /debug (no-arg shortcut)
    _last_pair[callback.from_user.id] = symbol

    # Quick hint from cache (pure computation, no browser/network calls)
    recommended_sec: int | None = None
    try:
        from services.candle_cache import get_cached
        from services.strategy_engine import calculate_signal
        candles = get_cached(symbol)
        if candles:
            result = await calculate_signal(candles)
            hint = (result.details or {}).get("expiry_hint", "")
            if hint == "1m":
                recommended_sec = 60
            elif hint == "2m":
                recommended_sec = 120
    except Exception:
        pass  # hint is optional, never block user

    await callback.message.edit_text(
        f"⏱ <b>{pair_label}</b>\n\nВыберите время экспирации сделки:",
        parse_mode="HTML",
        reply_markup=expiration_keyboard(symbol, recommended_sec),
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

    if not is_data_ready():
        remaining = data_ready_in_seconds()
        mins = remaining // 60
        secs = remaining % 60
        time_str = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
        await callback.message.edit_text(
            "📊 <b>Накапливаю данные для анализа...</b>\n\n"
            f"Готовность через: <b>{time_str}</b>\n\n"
            "Бот собирает историю свечей для точного анализа.\n"
            "<i>Сигналы станут доступны автоматически.</i>",
            parse_mode="HTML",
            reply_markup=back_to_menu_keyboard(),
        )
        return

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

        # ── Save to DB & start result watcher ─────────────────────────────────
        if signal.direction in ("BUY", "SELL"):
            d = signal.details if isinstance(signal.details, dict) else {}
            signal_price = d.get("debug", {}).get("last_close")
            strategy     = d.get("primary_strategy")

            if signal_price:
                meta = _extract_signal_meta(d)
                outcome_id = await save_signal_outcome(
                    user_id        = callback.from_user.id,
                    symbol         = symbol,
                    pair_label     = pair_label,
                    direction      = signal.direction,
                    confidence     = signal.confidence,
                    strategy       = strategy,
                    expiration_sec = expiration_sec,
                    signal_price   = signal_price,
                    **meta,
                )
                asyncio.create_task(track_outcome(
                    bot            = callback.bot,
                    chat_id        = callback.from_user.id,
                    outcome_id     = outcome_id,
                    symbol         = symbol,
                    pair_label     = pair_label,
                    direction      = signal.direction,
                    strategy       = strategy,
                    expiration_sec = expiration_sec,
                    signal_price   = signal_price,
                ))

    except Exception as e:
        logger.exception("Signal fetch error: %s", e)
        await callback.message.edit_text(
            "❌ <b>Ошибка получения сигнала</b>\n\n"
            "Не удалось подключиться к платформе. Попробуйте позже.",
            parse_mode="HTML",
            reply_markup=back_to_menu_keyboard(),
        )


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

    await callback.answer("🔄 Сканирую рынок...")

    try:
        if not is_warm_up_done():
            await callback.message.edit_text(
                "⏳ <b>Бот загружается...</b>\n\n"
                "Идёт начальный сбор данных по парам (~2–3 мин после запуска).\n\n"
                "<i>Подождите немного и нажмите «Обновить».</i>",
                parse_mode="HTML",
                reply_markup=recommended_pairs_keyboard([]),
            )
            return
        if not is_data_ready():
            remaining = data_ready_in_seconds()
            mins = remaining // 60
            secs = remaining % 60
            time_str = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
            await callback.message.edit_text(
                "📊 <b>Накапливаю данные для анализа...</b>\n\n"
                f"Готовность через: <b>{time_str}</b>\n\n"
                "Бот собирает историю свечей для точного анализа.\n"
                "<i>Сигналы станут доступны автоматически.</i>",
                parse_mode="HTML",
                reply_markup=recommended_pairs_keyboard([]),
            )
            return

        await callback.message.edit_text(
            "🔄 <b>Сканирую рынок...</b>",
            parse_mode="HTML",
        )

        pairs_map = _build_pairs_map()
        results   = await scan_pairs_fresh(pairs_map)

        if not results:
            await callback.message.edit_text(
                "⚠️ <b>Подходящих пар не найдено</b>\n\n"
                "<i>Нажмите «Обновить» через 1–2 минуты.</i>",
                parse_mode="HTML",
                reply_markup=recommended_pairs_keyboard([]),
            )
            return

        await callback.message.edit_text(
            "📊 <b>Рекомендованные пары</b>",
            parse_mode="HTML",
            reply_markup=recommended_pairs_keyboard(results),
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


# ─── /signal — quick scan ────────────────────────────────────────────────────

@router.message(Command("signal"))
async def cmd_signal(message: Message) -> None:
    user_id = message.from_user.id
    status  = await get_status(user_id)
    if status != "approved":
        await message.answer("❌ У вас нет доступа к боту.")
        return

    cached_results, cache_age = get_scan_cache()
    if cached_results is not None:
        age_str = f"{int(cache_age)}с"
        text    = format_scan_output(cached_results, scan_age_sec=cache_age)
        text   += f"\n\n<i>Последнее сканирование: {age_str} назад</i>"
        await message.answer(
            text, parse_mode="HTML",
            reply_markup=recommended_pairs_keyboard(cached_results),
        )
        return

    msg = await message.answer(
        "📊 <b>Сканирую пары...</b>\n\nАнализирую рынок, подождите секунду.",
        parse_mode="HTML",
    )

    try:
        if not is_warm_up_done():
            await msg.edit_text(
                "⏳ <b>Бот загружается...</b>\n\n"
                "Идёт начальный сбор данных по парам (~2–3 мин после запуска).\n\n"
                "<i>Подождите немного и попробуйте ещё раз.</i>",
                parse_mode="HTML",
                reply_markup=recommended_pairs_keyboard([]),
            )
            return
        if not is_data_ready():
            remaining = data_ready_in_seconds()
            mins = remaining // 60
            secs = remaining % 60
            time_str = f"{mins} мин {secs} сек" if mins else f"{secs} сек"
            await msg.edit_text(
                "📊 <b>Накапливаю данные для анализа...</b>\n\n"
                f"Готовность через: <b>{time_str}</b>\n\n"
                "Бот собирает историю свечей для точного анализа.\n"
                "<i>Сигналы станут доступны автоматически.</i>",
                parse_mode="HTML",
                reply_markup=recommended_pairs_keyboard([]),
            )
            return

        pairs_map = _build_pairs_map()
        results   = scan_all_pairs(pairs_map)

        if not results:
            await msg.edit_text(
                "⚠️ <b>Подходящих пар не найдено</b>\n\n"
                "Рынок сейчас в неопределённом состоянии.\n\n"
                "<i>Подождите 1–2 минуты и попробуйте снова.</i>",
                parse_mode="HTML",
                reply_markup=recommended_pairs_keyboard([]),
            )
            return

        text = format_scan_output(results)
        await msg.edit_text(
            text, parse_mode="HTML",
            reply_markup=recommended_pairs_keyboard(results),
        )

    except Exception as e:
        logger.exception("cmd_signal error: %s", e)
        await msg.edit_text(
            "❌ <b>Ошибка сканирования</b>\n\nПопробуйте позже.",
            parse_mode="HTML",
            reply_markup=back_to_menu_keyboard(),
        )


# ─── /help ───────────────────────────────────────────────────────────────────

@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    user_id = message.from_user.id
    status = await get_status(user_id)

    if status == "pending":
        await message.answer(
            "⏳ Ваша заявка на доступ ещё рассматривается.\n"
            "Используйте /start чтобы проверить статус.",
            parse_mode="HTML",
        )
        return

    if status == "denied":
        await message.answer("⛔ Доступ к боту запрещён.")
        return

    await message.answer(
        "ℹ️ <b>Как пользоваться ботом</b>\n\n"
        "1. <b>/signal</b> — быстрый скан всех OTC-пар и список лучших сигналов прямо сейчас\n"
        "2. <b>/start</b> — главное меню с полным выбором пары и экспирации\n"
        "3. <b>/stats</b> — ваша личная статистика: WR, число сделок, результаты по стратегиям\n\n"
        "<b>Как торговать по сигналу:</b>\n"
        "• Выберите пару в Pocket Option\n"
        "• Нажмите на неё в боте → выберите экспирацию\n"
        "• Бот рассчитает направление (BUY / SELL) и силу сигнала\n"
        "• Открывайте сделку сразу после получения — таймер уже идёт\n\n"
        "<b>Сила сигнала:</b>\n"
        "🟩🟩🟩🟩🟩 — сильная\n"
        "🟩🟩🟩🟩⬜ — хорошая\n"
        "🟩🟩🟩⬜⬜ — умеренная\n\n"
        "<i>Сигналы основаны на Price Action, уровнях поддержки/сопротивления и индикаторах.</i>",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


# ─── /daystats (admin) ───────────────────────────────────────────────────────

@router.message(Command("daystats"))
async def cmd_daystats(message: Message) -> None:
    if not _is_admin(message.from_user.id):
        return

    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    data = await get_daily_admin_stats(today)

    t = data["totals"]
    total   = t.get("total")   or 0
    users   = t.get("unique_users") or 0
    wins    = t.get("wins")    or 0
    losses  = t.get("losses")  or 0
    pending = t.get("pending") or 0
    winrate = t.get("winrate")
    buy_c   = t.get("buy_count")  or 0
    sell_c  = t.get("sell_count") or 0

    if total == 0:
        await message.answer(
            f"📊 <b>Статистика за {today}</b>\n\nСигналов за сегодня ещё не было.",
            parse_mode="HTML",
        )
        return

    wr_str = f"{winrate}%" if winrate is not None else "—"

    lines = [
        f"📊 <b>Статистика за {today} (UTC)</b>\n",
        f"Всего сигналов:    <b>{total}</b>",
        f"Уникальных юзеров: <b>{users}</b>",
        f"✅ Прибыльных:     <b>{wins}</b>",
        f"❌ Убыточных:      <b>{losses}</b>",
        f"⏳ В процессе:     <b>{pending}</b>",
        f"🎯 Точность:       <b>{wr_str}</b>",
        f"📈 BUY / SELL:     <b>{buy_c} / {sell_c}</b>",
    ]

    _strat_names = {
        "impulse":  "Импульс",
        "bounce":   "Отскок",
        "breakout": "Лож. пробой",
    }

    if data["by_strategy"]:
        lines.append("\n<b>По стратегиям:</b>")
        for s in data["by_strategy"]:
            name = _strat_names.get(s["strategy"] or "", s["strategy"] or "?")
            wr   = f"{s['winrate']}%" if s["winrate"] is not None else "—"
            lines.append(
                f"  {name}: {s['total']} сиг  {s['wins']}W/{s['losses']}L  ({wr})"
            )

    if data["by_pair"]:
        lines.append("\n<b>По парам (топ):</b>")
        for p in data["by_pair"]:
            wr = f"{p['winrate']}%" if p["winrate"] is not None else "—"
            lines.append(
                f"  {p['pair_label']}: {p['total']} сиг  {p['wins']}W/{p['losses']}L  ({wr})"
            )

    await message.answer("\n".join(lines), parse_mode="HTML")


# ─── /report ─────────────────────────────────────────────────────────────────

@router.message(Command("report"))
async def cmd_report(message: Message) -> None:
    """Admin-only: performance breakdown by strategy/mode/tier/expiry/confidence."""
    if not _is_admin(message.from_user.id):
        return

    # Parse optional days arg: /report 7  or  /report 30
    args  = (message.text or "").split()
    days: int | None = None
    if len(args) >= 2:
        try:
            days = int(args[1])
        except ValueError:
            pass

    rows = await get_performance_report(days=days)

    if not rows:
        period = f"последние {days} дн." if days else "всё время"
        await message.answer(
            f"📋 <b>Performance Report</b> ({period})\n\nДанных пока нет.",
            parse_mode="HTML",
        )
        return

    _STRAT_LABEL = {
        "ema_bounce":        "EMA Bounce",
        "level_bounce":      "Level Bounce",
        "squeeze_breakout":  "Squeeze BO",
        "micro_breakout":    "Micro BO",
        "rsi_reversal":      "RSI Rev",
        "divergence":        "Divergence",
        "unknown":           "Unknown",
    }

    period_label = f"последние {days} дн." if days else "всё время"
    lines = [f"📋 <b>Performance Report</b> ({period_label})\n"]

    # Group rows by strategy
    from itertools import groupby
    for strat, strat_rows in groupby(rows, key=lambda r: r["strategy"]):
        strat_rows = list(strat_rows)
        s_total = sum(r["total"] for r in strat_rows)
        s_wins  = sum(r["wins"]  for r in strat_rows)
        s_loss  = sum(r["losses"] for r in strat_rows)
        s_wr    = f"{round(s_wins / (s_wins + s_loss) * 100, 1)}%" if (s_wins + s_loss) > 0 else "—"
        label   = _STRAT_LABEL.get(strat, strat)
        lines.append(f"\n<b>{label}</b>  [{s_total} сиг  {s_wins}W/{s_loss}L  {s_wr}]")

        # Group by mode × tier
        def _mode_tier(r: dict) -> tuple:
            return (r["market_mode"], r["used_tier"])

        for (mode, tier), group in groupby(strat_rows, key=_mode_tier):
            group = list(group)
            g_wins = sum(r["wins"]  for r in group)
            g_loss = sum(r["losses"] for r in group)
            g_tot  = sum(r["total"] for r in group)
            g_wr   = f"{round(g_wins / (g_wins + g_loss) * 100, 1)}%" if (g_wins + g_loss) > 0 else "—"
            tier_label = {"primary": "1st", "secondary": "2nd"}.get(tier or "", tier or "?")
            lines.append(f"  {mode or '?'} [{tier_label}]  {g_tot} сиг  {g_wins}W/{g_loss}L  {g_wr}")

            # Detail rows: expiry + confidence band
            for r in group:
                band = r["confidence_band"]
                exp  = f"{r['expiration_sec'] // 60}м" if r["expiration_sec"] else "?"
                wr   = f"{r['win_rate']}%" if r["win_rate"] is not None else "—"
                lines.append(
                    f"    {exp}  ⭐{band}  {r['total']}сиг  {r['wins']}W/{r['losses']}L  {wr}"
                )

    # Split into chunks to respect Telegram's 4096-char message limit
    LIMIT = 4000
    chunk, length = [], 0
    for line in lines:
        length += len(line) + 1
        if length > LIMIT:
            await message.answer("\n".join(chunk), parse_mode="HTML")
            chunk, length = [line], len(line) + 1
        else:
            chunk.append(line)
    if chunk:
        await message.answer("\n".join(chunk), parse_mode="HTML")


# ─── /condstats — condition frequency report ──────────────────────────────────

@router.message(Command("condstats"))
async def cmd_condstats(message: Message) -> None:
    """Admin-only: show how often each condition passes per strategy."""
    if not _is_admin(message.from_user.id):
        return

    # Support /condstats reset
    args = (message.text or "").split()
    if len(args) >= 2 and args[1].lower() == "reset":
        await reset_condition_stats()
        await message.answer("✅ Статистика условий сброшена.", parse_mode="HTML")
        return

    data = await get_condition_stats()
    if not data:
        await message.answer(
            "📊 <b>Condition Stats</b>\n\nДанных пока нет — запусти /debug несколько раз.",
            parse_mode="HTML",
        )
        return

    _STRAT_LABELS = {
        "ema_bounce":       "EMA Bounce",
        "squeeze_breakout": "Squeeze BO",
        "level_bounce":     "Level Bounce",
        "divergence":       "Divergence",
    }

    # Total evaluations across all strategies (denominator for header)
    total_evals = sum(v["evaluated"] for v in data.values())
    lines = [f"📊 <b>Condition Stats</b>  (всего eval: {total_evals})\n"
             f"<i>Условия отсортированы: худшие сверху</i>"]

    for strat, info in sorted(data.items()):
        n_eval = info["evaluated"]
        label  = _STRAT_LABELS.get(strat, strat)
        lines.append(f"\n<b>{label}</b> — eval: {n_eval}")

        conds = info.get("conditions", {})
        if not conds:
            lines.append("  (нет данных об условиях)")
            continue

        for cname, cv in conds.items():
            t    = cv["true"]
            tot  = t + cv["false"]
            rate = cv["rate"]
            bar  = _mini_bar(rate)
            # Flag worst conditions
            flag = "🔴" if rate < 30 else ("🟡" if rate < 60 else "🟢")
            lines.append(f"  {flag} {cname}: {t}/{tot} ({rate}%) {bar}")

    lines.append(
        f"\n<i>Сброс статистики: /condstats reset</i>"
    )

    LIMIT = 4000
    chunk, length = [], 0
    for line in lines:
        length += len(line) + 1
        if length > LIMIT:
            await message.answer("\n".join(chunk), parse_mode="HTML")
            chunk, length = [line], len(line) + 1
        else:
            chunk.append(line)
    if chunk:
        await message.answer("\n".join(chunk), parse_mode="HTML")


def _mini_bar(rate: int) -> str:
    """Tiny ASCII progress bar out of 5 blocks."""
    filled = round(rate / 20)   # 0-5
    return "█" * filled + "░" * (5 - filled)


# ─── Admin management (/addadmin, /removeadmin, /admins) ─────────────────────

@router.message(Command("addadmin"))
async def cmd_addadmin(message: Message) -> None:
    if not _is_main_admin(message.from_user.id):
        return

    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer(
            "Использование: <code>/addadmin &lt;user_id&gt;</code>\n"
            "user_id можно узнать через @userinfobot",
            parse_mode="HTML",
        )
        return

    target_id = int(args[1].strip())

    if target_id == config.ADMIN_USER_ID:
        await message.answer("Это уже главный администратор.")
        return

    # Try to find username from users table
    import aiosqlite as _aio
    username = None
    async with _aio.connect(config.DB_PATH) as db:
        db.row_factory = _aio.Row
        async with db.execute("SELECT username FROM users WHERE user_id = ?", (target_id,)) as cur:
            row = await cur.fetchone()
            if row:
                username = row["username"] or None

    ok = await add_admin(target_id, username, added_by=message.from_user.id)
    if ok:
        _extra_admin_ids.add(target_id)
        name_str = f"@{username}" if username else str(target_id)
        await message.answer(f"✅ {name_str} ({target_id}) назначен администратором.")
        logger.info("Admin added: %s by %s", target_id, message.from_user.id)
    else:
        await message.answer(f"⚠️ Пользователь {target_id} уже является администратором.")


@router.message(Command("removeadmin"))
async def cmd_removeadmin(message: Message) -> None:
    if not _is_main_admin(message.from_user.id):
        return

    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer(
            "Использование: <code>/removeadmin &lt;user_id&gt;</code>",
            parse_mode="HTML",
        )
        return

    target_id = int(args[1].strip())

    if target_id == config.ADMIN_USER_ID:
        await message.answer("Нельзя удалить главного администратора.")
        return

    ok = await remove_admin(target_id)
    if ok:
        _extra_admin_ids.discard(target_id)
        await message.answer(f"✅ Пользователь {target_id} удалён из администраторов.")
        logger.info("Admin removed: %s by %s", target_id, message.from_user.id)
    else:
        await message.answer(f"⚠️ Пользователь {target_id} не найден в списке администраторов.")


@router.message(Command("admins"))
async def cmd_admins(message: Message) -> None:
    if not _is_main_admin(message.from_user.id):
        return

    admins = await get_all_admins()
    if not admins:
        await message.answer(
            "👥 <b>Администраторы</b>\n\n"
            "Назначенных администраторов нет.\n"
            "Добавить: <code>/addadmin &lt;user_id&gt;</code>",
            parse_mode="HTML",
        )
        return

    lines = ["👥 <b>Администраторы</b>\n"]
    for a in admins:
        name = f"@{a['username']}" if a.get("username") else str(a["user_id"])
        date = (a.get("added_at") or "")[:10]
        lines.append(f"• {name} (<code>{a['user_id']}</code>) — с {date}")

    lines.append(f"\nВсего: {len(admins)}")
    lines.append("Удалить: <code>/removeadmin &lt;user_id&gt;</code>")
    await message.answer("\n".join(lines), parse_mode="HTML")


# ─── /stats ──────────────────────────────────────────────────────────────────

@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    user_id = message.from_user.id
    status = await get_status(user_id)
    if status != "approved":
        await message.answer("❌ У вас нет доступа к боту.")
        return

    stats    = await get_user_stats(user_id)
    by_pair  = await get_pair_stats(user_id, limit=5)

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

    if by_pair:
        lines.append("\n<b>Лучшие пары:</b>")
        for p in by_pair:
            wr = f"{p['winrate']}%" if p["winrate"] is not None else "—"
            lines.append(f"  {p['pair_label']}: {p['wins']}W / {p['losses']}L  ({wr})")

    await message.answer("\n".join(lines), parse_mode="HTML")
