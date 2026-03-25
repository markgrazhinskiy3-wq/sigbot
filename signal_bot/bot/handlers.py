import asyncio
import html
import logging
from datetime import datetime

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
    get_pair_outcomes, get_last_trades, get_outcome_by_id,
)
from services.access_service import notify_admin_new_user, check_access
from services.signal_service import get_signal, format_signal_message
from services.candle_cache import is_warm_up_done, is_data_ready, data_ready_in_seconds
from services.outcome_tracker import track_outcome
from services import analytics_logger
from services.analysis.asset_scanner import (
    scan_all_pairs, scan_pairs_fresh, format_scan_output, get_scan_cache, TradabilityResult,
)
import services.pairs_cache as pairs_cache

# ── Active monitoring tasks: {user_id: asyncio.Task} ─────────────────────────
_monitor_tasks: dict[int, asyncio.Task] = {}

# ── Last selected pair per user (for /debug without args) ─────────────────────
_last_pair: dict[int, str] = {}

# ── Last FIRED signal per user — stored when BUY/SELL is sent ─────────────────
# Value: {"symbol": str, "pair_label": str, "direction": str,
#          "confidence": float, "details": dict, "fired_at": float}
_last_fired_signal: dict[int, dict] = {}
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
        expiry_str = "2m" if expiration_sec >= 120 else "1m"
        try:
            result: SignalResult = await calculate_signal(candles, expiry=expiry_str)
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
        outcome_id_snap = None
        if signal_price:
            meta = _extract_signal_meta(d)
            outcome_id_snap = await save_signal_outcome(
                user_id=chat_id, symbol=symbol, pair_label=pair_label,
                direction=signal.direction, confidence=signal.confidence,
                strategy=strategy, expiration_sec=exp,
                signal_price=signal_price,
                **meta,
            )
            asyncio.create_task(track_outcome(
                bot=bot, chat_id=chat_id, outcome_id=outcome_id_snap,
                symbol=symbol, pair_label=pair_label,
                direction=signal.direction, strategy=strategy,
                expiration_sec=exp, signal_price=signal_price,
            ))
            expiry_str = "2m" if exp >= 120 else "1m"
            asyncio.create_task(analytics_logger.log_signal(
                outcome_id=outcome_id_snap, pair=pair_label, symbol=symbol,
                direction=signal.direction, expiry=expiry_str,
                entry_price=signal_price, details=d,
            ))

        # Store for /debug (allows reviewing this signal after market moves on)
        import time as _time
        _last_fired_signal[chat_id] = {
            "symbol":     symbol,
            "pair_label": pair_label,
            "direction":  signal.direction,
            "confidence": signal.confidence,
            "details":    result.details if isinstance(result.details, dict) else {},
            "fired_at":   _time.time(),
            "outcome_id": outcome_id_snap,
        }

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
    /debug          — показывает анализ последнего BUY/SELL сигнала (снапшот момента сигнала)
    /debug SYMBOL   — запускает СВЕЖИЙ анализ указанной пары прямо сейчас
    Admin-only.
    """
    if not _is_admin(message.from_user.id):
        return

    import time as _time
    msg_parts  = message.text.split(maxsplit=1)
    raw_symbol = msg_parts[1].strip() if len(msg_parts) > 1 else None

    # ── /debug без аргументов → показать ПОСЛЕДНИЙ сохранённый сигнал ──────
    if not raw_symbol:
        stored = _last_fired_signal.get(message.from_user.id)
        if not stored:
            await message.answer(
                "ℹ️ Нет сохранённого сигнала.\n\n"
                "Сигнал сохраняется автоматически при каждом BUY/SELL.\n"
                "Для свежего анализа: <code>/debug SYMBOL</code>",
                parse_mode="HTML",
            )
            return
        details = stored["details"]
        debug   = details.get("debug", {}) if isinstance(details, dict) else {}
        age_sec = int(_time.time() - stored.get("fired_at", _time.time()))
        age_str = f"{age_sec // 60} мин {age_sec % 60} сек назад" if age_sec >= 60 else f"{age_sec} сек назад"
        symbol      = stored["symbol"]
        header_line = f"🔬 <b>DEBUG {symbol}</b>  <i>(сигнал: {age_str})</i>"
        _signal_conf = stored.get("confidence", "?")

        # Fetch trade outcome from DB if available
        _outcome_row = None
        _oid = stored.get("outcome_id")
        if _oid:
            try:
                _outcome_row = await get_outcome_by_id(_oid)
            except Exception:
                pass
    else:
        # ── /debug SYMBOL → свежий анализ ────────────────────────────────────
        symbol = raw_symbol
        _last_pair[message.from_user.id] = symbol
        await message.answer(f"🔬 Запускаю свежий анализ <code>{symbol}</code>...", parse_mode="HTML")
        try:
            pair_label  = _label_for_symbol(symbol)
            signal_resp = await get_signal(symbol=symbol, pair_label=pair_label, expiration_sec=60)
        except Exception as e:
            await message.answer(f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")
            return
        details = signal_resp.details if hasattr(signal_resp, "details") else {}
        debug   = details.get("debug", {}) if isinstance(details, dict) else {}
        header_line  = f"🔬 <b>DEBUG {symbol}</b>  <i>(свежий анализ)</i>"
        _signal_conf = signal_resp.confidence
        _outcome_row = None   # fresh analysis — no DB outcome to show

    if not debug:
        await message.answer("Нет данных debug (не удалось получить свечи).")
        return

    direction  = details.get("direction", "NO_SIGNAL")
    last_close = debug.get("last_close", "?")

    # v2 stores n_15s/n_1m/n_5m; v1 used n_bars_15s/n_bars_1m/n_bars_5m
    n15 = debug.get("n_bars_15s") or debug.get("n_15s") or debug.get("candles_raw", "?")
    n1m = debug.get("n_bars_1m") or debug.get("n_1m", 0)
    n5m = debug.get("n_bars_5m") or debug.get("n_5m", 0)
    nc  = debug.get("candles_clean", "?")
    abp = debug.get("avg_body_pct", 0)

    lines = [
        header_line,
        f"Цена: <code>{last_close}</code> | Направление: <b>{direction}</b>",
        "",
        f"📊 <b>Свечи</b>",
        f"  15s bars: <b>{n15}</b> raw → <b>{nc}</b> clean",
        f"  1m bars:  <b>{n1m}</b> | 5m bars: <b>{n5m}</b>",
        f"  avg_body: {float(abp):.5f}%",
    ]

    # Market mode
    mode     = debug.get("mode") or details.get("market_mode", "?")
    mode_str = debug.get("mode_strength", "?")
    mode_exp = debug.get("mode_explanation", "")
    mode_dbg = debug.get("mode_debug") or {}
    lines += ["", f"🧭 <b>Режим рынка: {mode}</b> (сила={mode_str})"]
    if mode_exp:
        lines.append(f"  {html.escape(str(mode_exp))}")
    if isinstance(mode_dbg, dict) and mode_dbg:
        dbg_parts = [f"{html.escape(str(k))}={html.escape(str(v))}" for k, v in mode_dbg.items()]
        lines.append(f"  {' | '.join(dbg_parts)}")

    # Indicators
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
        spread_str = "?"
        if isinstance(e5, float) and isinstance(e21, float) and isinstance(last_close, float) and last_close:
            spread_str = f"{(e5 - e21) / last_close * 100:+.4f}%"
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

    # Levels
    lvl = debug.get("levels", {})
    if lvl:
        lines += [
            "",
            "📌 <b>Уровни</b>",
            f"  Ближ. суп: {lvl.get('nearest_sup','?')}  ({lvl.get('dist_sup_pct','?')}% от цены)",
            f"  Ближ. рез: {lvl.get('nearest_res','?')}  ({lvl.get('dist_res_pct','?')}% от цены)",
            f"  Кол-во:    {lvl.get('n_supports',0)} суп  /  {lvl.get('n_resistances',0)} рез",
        ]

    # Context
    cup  = debug.get("ctx_up_1m",  debug.get("ctx_up",   False))
    cdn  = debug.get("ctx_dn_1m",  debug.get("ctx_down", False))
    mnot = debug.get("ctx_macro_note", "—")
    lines += [
        "",
        "🔗 <b>Контекст MTF</b>",
        f"  1m EMA:   {'↑' if cup else '↓' if cdn else '—'}",
        f"  Macro:    {html.escape(str(mnot))}",
    ]

    # Strategies
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
            for cname, cval in sd.get("conditions", {}).items():
                if isinstance(cval, bool):
                    lines.append(f"    {'✅' if cval else '❌'} {cname}")
                elif isinstance(cval, str):
                    lines.append(f"    ℹ️ {cname}: {html.escape(cval)}")

    # Final decision
    lines.append("")
    if direction in ("BUY", "SELL"):
        conf_raw = details.get("confidence_raw", debug.get("conf_after_multipliers", "?"))
        conf_5   = details.get("confidence_5", _signal_conf)
        quality  = details.get("signal_quality", "?")
        expiry   = details.get("expiry_hint", "?")
        strat    = details.get("primary_strategy", "?")
        lines += [
            f"✅ <b>Сигнал: {direction}</b>",
            f"  Стратегия: {strat} [{used_tier}]",
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

    # ── Trade outcome (only for stored signals with a DB record) ──────────────
    if _outcome_row:
        o_status = _outcome_row.get("outcome", "pending")
        s_price  = _outcome_row.get("signal_price")
        r_price  = _outcome_row.get("result_price")
        exp_s    = _outcome_row.get("expiration_sec", 0)
        exp_str  = f"{exp_s // 60}м" if exp_s else "?"

        lines.append("")
        if o_status == "win":
            pct_move = abs((r_price - s_price) / s_price * 100) if s_price and r_price else 0
            lines.append(f"📊 <b>Результат сделки: ✅ ПРОФИТ</b>  (экспирация {exp_str})")
            lines.append(f"  Вход: {s_price}  →  Закрытие: {r_price}  ({pct_move:+.4f}%)")
        elif o_status == "loss":
            pct_move = (r_price - s_price) / s_price * 100 if s_price and r_price else 0
            lines.append(f"📊 <b>Результат сделки: ❌ УБЫТОК</b>  (экспирация {exp_str})")
            lines.append(f"  Вход: {s_price}  →  Закрытие: {r_price}  ({pct_move:+.4f}%)")
        elif o_status == "pending":
            lines.append(f"📊 <b>Результат сделки: ⏳ ожидание</b>  (экспирация {exp_str})")
        else:
            lines.append(f"📊 <b>Результат сделки: ⚠️ ошибка проверки</b>")
    elif stored.get("outcome_id") is None and not raw_symbol:
        # Signal was fired but signal_price wasn't available — outcome not tracked
        lines.append("")
        lines.append("📊 <i>Результат не отслеживается (цена входа не определена)</i>")

    text = "\n".join(lines)
    LIMIT = 4000
    if len(text) <= LIMIT:
        await message.answer(text, parse_mode="HTML")
    else:
        chunk: list[str] = []
        length = 0
        for line in lines:
            length += len(line) + 1
            if length > LIMIT:
                await message.answer("\n".join(chunk), parse_mode="HTML")
                chunk, length = [line], len(line) + 1
            else:
                chunk.append(line)
        if chunk:
            await message.answer("\n".join(chunk), parse_mode="HTML")


# ─── /pairsinfo — show pairs cache with payout (admin only) ─────────────────

@router.message(Command("pairsinfo"))
async def cmd_pairsinfo(message: Message) -> None:
    """Show the current pairs cache with payout % for debugging."""
    if not _is_primary_admin(message.from_user.id):
        return

    cached = pairs_cache.get_cached()
    is_live = pairs_cache.is_fresh()

    lines = [
        f"📊 <b>Кэш пар</b> ({'живые данные' if is_live else 'fallback из конфига'})\n",
    ]
    for p in cached:
        payout = _extract_payout(p)
        sym    = p.get("symbol", "?")
        name   = p.get("name") or p.get("label", "?").split("|")[0].strip()
        payout_str = f"  →  <b>{payout}%</b>" if payout > 0 else "  →  нет данных"
        lines.append(f"• <code>{sym}</code>  {name}{payout_str}")

    lines.append(f"\nВсего: {len(cached)} пар")
    await message.answer("\n".join(lines), parse_mode="HTML")


# ─── /pairsdiag — WS payout diagnostic (admin only) ─────────────────────────

@router.message(Command("pairsdiag"))
async def cmd_pairsdiag(message: Message) -> None:
    """
    Admin: probe PocketOption WS for payout data and report every event seen.
    Runs for ~12 seconds. Use to identify the correct WS event for payout.
    """
    await message.answer("🔍 <b>WS-диагностика пayout</b>\nЗондирую PocketOption (~12 сек)…", parse_mode="HTML")

    try:
        import json as _json
        import asyncio as _asyncio
        import time as _time
        import aiohttp as _aiohttp
        from services.po_ws_client import (
            load_auth, _reader, _keepalive, _handshake, get_live_payouts,
        )

        # Show what's already captured from candle fetches
        live = get_live_payouts()
        live_info = (
            "\n".join(f"  {k}: {v}%" for k, v in list(live.items())[:10])
            if live else "  (пусто)"
        )

        # Probe WS and collect all text events
        auth_data = load_auth()
        if not auth_data:
            await message.answer("❌ WS auth ещё не захвачен браузером. Подождите ~1 мин после старта.", parse_mode="HTML")
            return

        ws_url       = auth_data["ws_url"]
        auth_payload = auth_data["auth"]
        headers = {
            "Origin":  "https://pocketoption.com",
            "Referer": "https://pocketoption.com/en/cabinet/demo-quick-high-low/",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
        }

        events: list[str] = []  # "event_name: preview"
        timeout = 12.0

        async with _aiohttp.ClientSession() as http:
            async with http.ws_connect(
                ws_url, headers=headers, heartbeat=None,
                receive_timeout=None, autoclose=False, autoping=False,
            ) as ws:
                queue: _asyncio.Queue = _asyncio.Queue()
                reader_task = _asyncio.create_task(_reader(ws, queue))
                ping_task   = _asyncio.create_task(_keepalive(ws, 25.0))
                try:
                    pi = await _handshake(ws, auth_payload, queue)
                    ping_task.cancel()
                    ping_task = _asyncio.create_task(_keepalive(ws, pi))

                    # Candidate messages
                    for msg in ['42["assets"]', '42["getAssets"]', '42["updateAssets"]',
                                '42["assets/load"]', '42["openOptions"]']:
                        try: await ws.send_str(msg)
                        except Exception: pass

                    # Subscribe to first 3 pairs
                    for p in config.OTC_PAIRS[:3]:
                        asset = p["symbol"].lstrip("#")
                        await ws.send_str("42" + _json.dumps(["changeSymbol", {"asset": asset, "period": 60}]))

                    # Drain
                    seen_events: set[str] = set()
                    deadline = _time.monotonic() + timeout
                    while _time.monotonic() < deadline:
                        remaining = deadline - _time.monotonic()
                        try:
                            item = await _asyncio.wait_for(queue.get(), timeout=min(remaining, 1.5))
                        except _asyncio.TimeoutError:
                            continue
                        if item is None:
                            break
                        if item[0] != "text":
                            continue
                        text = item[1]
                        if not text.startswith("42"):
                            continue
                        try:
                            data = _json.loads(text[2:])
                        except Exception:
                            continue
                        if not isinstance(data, list) or len(data) < 1:
                            continue
                        ev = str(data[0])
                        pl = data[1] if len(data) > 1 else {}
                        if ev not in seen_events:
                            seen_events.add(ev)
                            preview = str(pl)[:120]
                            events.append(f"<code>{html.escape(ev)}</code>: {html.escape(preview)}")
                finally:
                    reader_task.cancel()
                    ping_task.cancel()

        lines = [
            "📡 <b>WS Payout Diagnostic</b>\n",
            f"<b>_live_payouts (из свечей):</b>\n{live_info}\n",
            f"<b>Все text-события за 12 сек ({len(events)} уникальных):</b>",
        ] + events + [
            "\n<i>Если нет событий с profit/payout — PO не отправляет % через WS.</i>"
        ]
        text_out = "\n".join(lines)
        # Telegram limit 4096
        for chunk in [text_out[i:i+4000] for i in range(0, len(text_out), 4000)]:
            await message.answer(chunk, parse_mode="HTML")

    except Exception as exc:
        await message.answer(f"❌ Ошибка диагностики: <code>{html.escape(str(exc))}</code>", parse_mode="HTML")


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
    # Always refresh — reads live payouts from memory (no network call), nearly instant.
    # This guarantees the payout % shown is always current.
    await pairs_cache.refresh(force=True)

    all_pairs = pairs_cache.get_cached()
    pairs = all_pairs

    # Check if live payout data is available from WS updateAssets
    try:
        from services.po_ws_client import get_live_assets
        has_live = bool(get_live_assets())
    except Exception:
        has_live = False

    if has_live:
        header = (
            "📊 <b>Доступные OTC-пары</b>\n"
            "<i>Только с выплатой ≥80% · Обновлено сейчас</i>"
        )
    else:
        header = (
            "📊 <b>Доступные OTC-пары</b>\n"
            "<i>Выплата ≥80% · Список обновляется автоматически</i>"
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

        # ── Save to DB, start result watcher, store /debug snapshot ──────────
        if signal.direction in ("BUY", "SELL"):
            import time as _time
            d = signal.details if isinstance(signal.details, dict) else {}
            signal_price = d.get("debug", {}).get("last_close")
            strategy     = d.get("primary_strategy")

            outcome_id_snap = None
            if signal_price:
                meta = _extract_signal_meta(d)
                outcome_id_snap = await save_signal_outcome(
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
                    outcome_id     = outcome_id_snap,
                    symbol         = symbol,
                    pair_label     = pair_label,
                    direction      = signal.direction,
                    strategy       = strategy,
                    expiration_sec = expiration_sec,
                    signal_price   = signal_price,
                ))
                _expiry_str = "2m" if expiration_sec >= 120 else "1m"
                asyncio.create_task(analytics_logger.log_signal(
                    outcome_id=outcome_id_snap, pair=pair_label, symbol=symbol,
                    direction=signal.direction, expiry=_expiry_str,
                    entry_price=signal_price, details=d,
                ))

            _last_fired_signal[callback.from_user.id] = {
                "symbol":     symbol,
                "pair_label": pair_label,
                "direction":  signal.direction,
                "confidence": signal.confidence,
                "details":    d,
                "fired_at":   _time.time(),
                "outcome_id": outcome_id_snap,
            }

    except Exception as e:
        logger.exception("Signal fetch error: %s", e)
        await callback.message.edit_text(
            "❌ <b>Ошибка получения сигнала</b>\n\n"
            "Не удалось подключиться к платформе. Попробуйте позже.",
            parse_mode="HTML",
            reply_markup=back_to_menu_keyboard(),
        )


# ─── Recommended pairs ───────────────────────────────────────────────────────


def _extract_payout(p: dict) -> int:
    """Extract payout % from pair dict: use 'payout' field, or parse from label 'Name | 82%'."""
    import re as _re
    payout = int(p.get("payout") or 0)
    if payout == 0:
        m = _re.search(r'\|\s*(\d+)\s*%', p.get("label", ""))
        if m:
            payout = int(m.group(1))
    return payout


def _build_pairs_map() -> tuple[dict[str, str], dict[str, int]]:
    """
    Return (pairs_map, payout_map) built ONLY from the live cache.
    Relies on pairs_cache.get_cached() returning the dynamic live list
    (all currency OTC pairs with payout >= MIN_PAYOUT from updateAssets).
    No config fallback — avoids adding low-payout pairs back into the scan.
    """
    pairs_map:  dict[str, str] = {}
    payout_map: dict[str, int] = {}
    for p in pairs_cache.get_cached():
        sym = p["symbol"]
        pairs_map[sym]  = p.get("name") or p["label"].split("|")[0].strip()
        payout_map[sym] = _extract_payout(p)
    return pairs_map, payout_map


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

        await pairs_cache.refresh(force=True)
        pairs_map, payout_map = _build_pairs_map()
        results   = await scan_pairs_fresh(pairs_map, payout_map)

        if not results:
            await callback.message.edit_text(
                "⚠️ <b>Подходящих пар не найдено</b>\n\n"
                "<i>Нажмите «Обновить» через 1–2 минуты.</i>",
                parse_mode="HTML",
                reply_markup=recommended_pairs_keyboard([]),
            )
            return

        text = format_scan_output(results)
        await callback.message.edit_text(
            text,
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

        pairs_map, payout_map = _build_pairs_map()
        results   = scan_all_pairs(pairs_map, payout_map)

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
        "ema_bounce":     "EMA Bounce",
        "level_bounce":   "Level Bounce",
        "level_breakout": "Level Breakout",
        "rsi_reversal":   "RSI Rev",
        "unknown":        "Unknown",
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
        "ema_bounce":     "EMA Bounce",
        "level_bounce":   "Level Bounce",
        "level_breakout": "Level Breakout",
        "rsi_reversal":   "RSI Rev",
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


# ─── Analytics (/analytics, /export_csv) ──────────────────────────────────────

@router.message(Command("analytics"))
async def cmd_analytics(message: Message) -> None:
    """Admin-only: show winrate summary from signals_log."""
    if not _is_admin(message.from_user.id):
        return

    summary = await analytics_logger.get_summary()
    if not summary or summary.get("total", 0) == 0:
        await message.answer(
            "📊 <b>Analytics</b>\n\nПока нет залогированных сигналов.\n"
            "Сигналы логируются автоматически при каждом BUY/SELL.",
            parse_mode="HTML",
        )
        return

    total    = summary["total"]
    resolved = summary["resolved"]
    wins     = summary["wins"]
    losses   = summary["losses"]
    wr       = summary["winrate"]
    pending  = total - resolved

    wr_str = f"{wr}%" if wr is not None else "—"

    lines = [
        "📊 <b>Analytics — signals_log</b>\n",
        f"Всего сигналов:  <b>{total}</b>",
        f"Разрешено:       <b>{resolved}</b>  (pending: {pending})",
        f"WIN / LOSS:      <b>{wins} / {losses}</b>",
        f"Общий winrate:   <b>{wr_str}</b>",
    ]

    by_pattern = summary.get("by_pattern", [])
    if by_pattern:
        lines.append("\n<b>По паттерну:</b>")
        for row in by_pattern:
            n = row["n"]
            w = row["w"]
            pat_wr = round(w / n * 100, 1) if n else 0
            bar = _mini_bar(pat_wr)
            lines.append(
                f"  {row['pattern_winner'] or '—': <22} {w}/{n}  ({pat_wr}%)  {bar}"
            )

    by_expiry = summary.get("by_expiry", [])
    if by_expiry:
        lines.append("\n<b>По экспирации:</b>")
        for row in by_expiry:
            n = row["n"]
            w = row["w"]
            exp_wr = round(w / n * 100, 1) if n else 0
            bar = _mini_bar(exp_wr)
            lines.append(f"  {row['expiry']}: {w}/{n} ({exp_wr}%) {bar}")

    lines.append("\n<i>Экспорт в CSV: /export_csv</i>")

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("export_csv"))
async def cmd_export_csv(message: Message) -> None:
    """Admin-only: export signals_log to CSV and send as file."""
    if not _is_admin(message.from_user.id):
        return

    import tempfile
    from aiogram.types import FSInputFile

    await message.answer("⏳ Генерирую CSV экспорт...", parse_mode="HTML")

    with tempfile.NamedTemporaryFile(
        suffix=".csv", prefix="signals_export_", delete=False, mode="w"
    ) as tmp:
        filepath = tmp.name

    n_rows = await analytics_logger.export_csv(filepath)

    if n_rows == 0:
        await message.answer("📊 <b>Нет данных для экспорта</b>\n\nЗапусти несколько сигналов сначала.", parse_mode="HTML")
        return

    try:
        doc = FSInputFile(filepath, filename="signals_log.csv")
        await message.answer_document(
            doc,
            caption=(
                f"📊 <b>signals_log.csv</b>\n"
                f"Строк: <b>{n_rows}</b>\n"
                f"<i>Используй в Excel / Python pandas для анализа winrate.</i>"
            ),
            parse_mode="HTML",
        )
    finally:
        try:
            os.unlink(filepath)
        except Exception:
            pass


# ─── Paper Trading Test (/paper_test) ────────────────────────────────────────

# Active paper test tasks per admin user_id
_paper_test_tasks: dict[int, asyncio.Task] = {}


@router.message(Command("paper_test"))
async def cmd_paper_test(message: Message) -> None:
    """
    Admin-only: run a silent paper trading session.
    Usage: /paper_test [n]  (default n=100)
    Usage: /paper_test stop  — cancel running session

    Scans all OTC pairs in real-time using the SAME engine as live trading.
    No signals are sent to users. Results logged to signals_log (source='paper').
    Summary printed after target trades are resolved.
    """
    if not _is_admin(message.from_user.id):
        return

    uid  = message.from_user.id
    args = (message.text or "").split()

    # ── Stop command ───────────────────────────────────────────────────────────
    if len(args) >= 2 and args[1].lower() == "stop":
        task = _paper_test_tasks.pop(uid, None)
        if task and not task.done():
            task.cancel()
            await message.answer("⏹ Paper test остановлен.", parse_mode="HTML")
        else:
            await message.answer("Нет активного paper test.", parse_mode="HTML")
        return

    # ── Start command ──────────────────────────────────────────────────────────
    # Cancel any existing task for this user
    old_task = _paper_test_tasks.pop(uid, None)
    if old_task and not old_task.done():
        old_task.cancel()

    try:
        target = int(args[1]) if len(args) >= 2 else 100
        target = max(1, min(target, 500))
    except ValueError:
        target = 100

    expiry = "both"
    if len(args) >= 3 and args[2] in ("1m", "2m"):
        expiry = args[2]

    await message.answer(
        f"🧪 <b>Paper Trading Test запущен</b>\n\n"
        f"Цель: <b>{target} сделок</b>\n"
        f"Экспирация: <b>{expiry}</b>\n"
        f"Все пары OTC сканируются в тихом режиме.\n"
        f"Сигналы <b>НЕ</b> отправляются пользователям.\n\n"
        f"Прогресс каждые 10 сделок.\n"
        f"Остановить: /paper_test stop",
        parse_mode="HTML",
    )

    bot = message.bot

    async def _progress(msg: str) -> None:
        try:
            await bot.send_message(uid, msg, parse_mode="HTML")
        except Exception:
            pass

    async def _run_paper() -> None:
        try:
            from backtest.paper_runner import run_paper_test
            results, summary = await run_paper_test(
                target      = target,
                expiry      = expiry,
                progress_cb = _progress,
            )
            # Send summary in chunks (Telegram 4096 char limit)
            _LIMIT = 3800
            lines  = summary.split("\n")
            chunk, length = [], 0
            for line in lines:
                length += len(line) + 1
                if length > _LIMIT:
                    await bot.send_message(
                        uid, "<pre>" + "\n".join(chunk) + "</pre>",
                        parse_mode="HTML",
                    )
                    chunk, length = [line], len(line) + 1
                else:
                    chunk.append(line)
            if chunk:
                await bot.send_message(
                    uid, "<pre>" + "\n".join(chunk) + "</pre>",
                    parse_mode="HTML",
                )
            # Export CSV — only trades from this session (not historical DB rows)
            import tempfile, csv
            from aiogram.types import FSInputFile
            if results:
                with tempfile.NamedTemporaryFile(
                    suffix=".csv", prefix="paper_export_", delete=False,
                    mode="w", newline="", encoding="utf-8",
                ) as tmp:
                    fpath = tmp.name
                    writer = csv.writer(tmp)
                    writer.writerow([
                        "pair", "symbol", "direction", "expiry",
                        "entry_price", "close_price", "result", "pnl_pct",
                        "entry_time", "strategy", "confidence",
                    ])
                    for r in results:
                        dbg  = r.trade.details.get("debug", {})
                        writer.writerow([
                            r.trade.pair,
                            r.trade.symbol,
                            r.trade.direction,
                            r.trade.expiry,
                            r.trade.entry_price,
                            r.close_price,
                            r.result,
                            r.pnl_pct,
                            datetime.fromtimestamp(r.trade.entry_time).strftime("%Y-%m-%d %H:%M:%S"),
                            r.trade.details.get("primary_strategy", ""),
                            dbg.get("final_score") or r.trade.details.get("confidence_raw", ""),
                        ])
                try:
                    doc = FSInputFile(fpath, filename="paper_signals_log.csv")
                    await bot.send_document(
                        uid, doc,
                        caption=(
                            f"📊 <b>Paper signals_log.csv</b>\n"
                            f"Строк (сессия): <b>{len(results)}</b>"
                        ),
                        parse_mode="HTML",
                    )
                finally:
                    try:
                        os.unlink(fpath)
                    except Exception:
                        pass

        except asyncio.CancelledError:
            logger.info("Paper test cancelled for user %d", uid)
        except Exception as exc:
            logger.exception("Paper test error for user %d: %s", uid, exc)
            try:
                await bot.send_message(uid, f"❌ Paper test ошибка: {exc}", parse_mode="HTML")
            except Exception:
                pass
        finally:
            _paper_test_tasks.pop(uid, None)

    task = asyncio.create_task(_run_paper())
    _paper_test_tasks[uid] = task


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
