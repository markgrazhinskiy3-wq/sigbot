import html
import logging
from dataclasses import dataclass

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from services.candle_cache import get_candles_cached as get_candles, get_cached_symbols, refresh_pair_now
from services.strategy_engine import calculate_signal, SignalResult

logger = logging.getLogger(__name__)


@dataclass
class SignalResponse:
    direction: str       # "BUY" | "SELL" | "NO_SIGNAL"
    confidence: int
    details: dict
    pair: str
    expiration_sec: int
    symbol: str = ""     # raw symbol e.g. "#AUDCAD_otc"


async def scan_all_signals(pairs_map: dict[str, str]) -> list[SignalResponse]:
    """
    Scan all cached pairs and return a list of BUY/SELL signals sorted by confidence desc.
    `pairs_map` is {symbol: label}. Returns empty list if no signals found.

    Uses get_cached (fresh cache only) — never triggers live browser fetches.
    If a pair's cache is expired or empty, it is skipped silently.
    """
    from services.candle_cache import get_cached
    symbols = get_cached_symbols()
    results: list[SignalResponse] = []
    for symbol in symbols:
        candles = get_cached(symbol)   # cache-only: returns None if expired/missing
        if not candles:
            continue
        label = pairs_map.get(symbol, symbol)
        result: SignalResult = calculate_signal(candles)
        if result.direction not in ("BUY", "SELL"):
            continue
        results.append(SignalResponse(
            direction=result.direction,
            confidence=result.confidence,
            details=result.details,
            pair=label,
            expiration_sec=_expiry_seconds(result.details),
            symbol=symbol,
        ))
    results.sort(key=lambda r: r.confidence, reverse=True)
    return results


async def get_signal(symbol: str, pair_label: str, expiration_sec: int) -> SignalResponse:
    logger.info("Fetching fresh signal for %s (exp=%ds)", symbol, expiration_sec)
    candles = await refresh_pair_now(symbol)
    if not candles:
        candles = await get_candles(symbol, count=80)
    result: SignalResult = calculate_signal(candles)
    return SignalResponse(
        direction=result.direction,
        confidence=result.confidence,
        details=result.details,
        pair=pair_label,
        expiration_sec=_expiry_seconds(result.details) if expiration_sec == 0 else expiration_sec,
        symbol=symbol,
    )


def _expiry_seconds(details: dict) -> int:
    """Convert expiry_hint ('1m'/'2m') to seconds."""
    hint = details.get("expiry_hint", "1m")
    return 120 if hint == "2m" else 60


def _conf_bar(confidence: int, total: int = 5) -> str:
    filled = min(confidence, total)
    return "🟩" * filled + "⬜" * (total - filled)


def _conf_label(confidence: int) -> str:
    if confidence >= 5:
        return "максимальная"
    if confidence >= 4:
        return "высокая"
    if confidence >= 3:
        return "средняя"
    if confidence >= 2:
        return "умеренная"
    return "низкая"


def _mode_label(mode: str) -> str:
    return {
        "TRENDING_UP":   "📈 Восходящий тренд",
        "TRENDING_DOWN": "📉 Нисходящий тренд",
        "RANGE":         "↔️ Боковой рынок",
        "VOLATILE":      "🌪 Волатильный рынок",
        "SQUEEZE":       "🗜 Сжатие (ожидание пробоя)",
    }.get(mode, "")


def format_signal_message(signal: SignalResponse) -> str:
    if signal.direction == "NO_SIGNAL":
        d      = signal.details if isinstance(signal.details, dict) else {}
        reject = d.get("reject_reason") or ""
        mode   = d.get("market_mode", "")
        hard   = d.get("hard_conflicts", [])
        mode_lbl = _mode_label(mode) or d.get("regime_label", "")

        lines = [
            f"🔍 <b>{html.escape(signal.pair)}</b>",
            "",
            f"⏳ <b>Сигнал ещё не сформировался</b>" + (f" — {mode_lbl}" if mode_lbl else ""),
            "Условия благоприятные, но конкретной точки входа пока нет.",
        ]
        if hard and hard[0]:
            lines.append(f"\n<i>🚫 {html.escape(str(hard[0]))}</i>")
        elif reject:
            lines.append(f"\n<i>{html.escape(str(reject))}</i>")
        lines.append("")
        lines.append("🤖 <b>Мониторинг запущен.</b> Бот будет проверять эту пару каждые 25 секунд и пришлёт уведомление как только появится сигнал.")
        lines.append("")
        lines.append("<i>Нажмите «Назад», чтобы выбрать другую пару.</i>")
        return "\n".join(lines)

    if signal.direction == "BUY":
        arrow     = "⬆️"
        dir_label = "BUY ⬆️ (ВВЕРХ)"
    else:
        arrow     = "⬇️"
        dir_label = "SELL ⬇️ (ВНИЗ)"

    bar   = _conf_bar(signal.confidence)
    label = _conf_label(signal.confidence)

    d = signal.details if isinstance(signal.details, dict) else {}
    mode     = d.get("market_mode", "")
    mode_lbl = _mode_label(mode)
    strategy = d.get("primary_strategy", "")
    # Always show the user-selected expiration, not the engine's hint
    exp_sec  = signal.expiration_sec or 60
    expiry   = f"{exp_sec // 60} мин" if exp_sec >= 60 else f"{exp_sec} сек"
    explanation_lines = _build_explanation(signal.direction, d)

    lines = [
        f"{arrow} <b>{html.escape(signal.pair)}</b>",
        "",
        f"📊 Сигнал: <b>{dir_label}</b>",
        f"💪 Уверенность: {bar} {signal.confidence}/5 ({label})",
    ]
    if mode_lbl:
        lines.append(f"🏷 Режим: {mode_lbl}")
    lines.append(f"⏱ Экспирация: <b>{expiry}</b>")
    lines.append("")

    if explanation_lines:
        lines.append("<b>Почему:</b>")
        for item in explanation_lines:
            lines.append(f"• {item}")
        lines.append("")

    lines.append("<i>Откройте сделку вручную на Pocket Option.</i>")
    return "\n".join(lines)


def _build_explanation(direction: str, details: dict) -> list[str]:
    """
    Build 2-4 bullet points explaining the signal in plain Russian.
    """
    is_buy   = direction == "BUY"
    strategy = details.get("primary_strategy") or ""
    mode     = details.get("market_mode", "")
    quality  = details.get("signal_quality", "")
    reasoning = details.get("reasoning", "")
    debug    = details.get("debug", {})

    items: list[str] = []

    # 1. Strategy-specific main reason
    if strategy == "ema_bounce":
        if is_buy:
            items.append("Цена откатилась к скользящей средней и оттолкнулась вверх — тренд продолжается.")
        else:
            items.append("Цена откатилась к скользящей средней и оттолкнулась вниз — тренд продолжается.")

    elif strategy == "squeeze_breakout":
        if is_buy:
            items.append("После периода сжатия (маленьких свечей) рынок резко вырвался вверх — импульс нарастает.")
        else:
            items.append("После периода сжатия (маленьких свечей) рынок резко вырвался вниз — импульс нарастает.")

    elif strategy == "level_bounce":
        if is_buy:
            items.append("Цена опустилась до уровня поддержки (где раньше разворачивалась) и отскочила вверх.")
        else:
            items.append("Цена поднялась до уровня сопротивления (где раньше разворачивалась) и отскочила вниз.")

    elif strategy == "rsi_reversal":
        if is_buy:
            items.append("Рынок чрезмерно упал (RSI в экстремуме) — ожидаем краткосрочный отскок вверх.")
        else:
            items.append("Рынок чрезмерно вырос (RSI в экстремуме) — ожидаем краткосрочный откат вниз.")

    elif strategy == "micro_breakout":
        if is_buy:
            items.append("Цена пробила уровень сопротивления после нескольких проверок — прорыв вверх.")
        else:
            items.append("Цена пробила уровень поддержки после нескольких проверок — прорыв вниз.")

    elif strategy == "divergence":
        if is_buy:
            items.append("Цена сделала новый минимум, но индикаторы не подтвердили — бычья дивергенция.")
        else:
            items.append("Цена сделала новый максимум, но индикаторы не подтвердили — медвежья дивергенция.")

    else:
        if is_buy:
            items.append("Большинство факторов указывают на движение вверх.")
        else:
            items.append("Большинство факторов указывают на движение вниз.")

    # 2. Market mode context
    if mode == "TRENDING_UP" and is_buy:
        items.append("Работаем в направлении восходящего тренда — вход по движению.")
    elif mode == "TRENDING_DOWN" and not is_buy:
        items.append("Работаем в направлении нисходящего тренда — вход по движению.")
    elif mode == "RANGE":
        if is_buy:
            items.append("Рынок в боковике — торгуем от нижней границы диапазона.")
        else:
            items.append("Рынок в боковике — торгуем от верхней границы диапазона.")
    elif mode == "VOLATILE":
        items.append("Высокая волатильность — быстрый вход, короткая экспирация.")
    elif mode == "SQUEEZE":
        items.append("После сжатия рынок обычно делает резкое движение — мы в начале него.")

    # 3. Indicator context from debug
    ind = debug.get("indicators", {})
    rsi_val = ind.get("rsi", 50)
    if is_buy and rsi_val < 35:
        items.append(f"Индикатор RSI ({rsi_val:.0f}) подтверждает перепроданность.")
    elif not is_buy and rsi_val > 65:
        items.append(f"Индикатор RSI ({rsi_val:.0f}) подтверждает перекупленность.")
    elif 40 < rsi_val < 60:
        items.append(f"RSI в нейтральной зоне ({rsi_val:.0f}) — место для движения есть.")

    # 4. Quality note
    if quality == "strong":
        items.append("Несколько условий совпали одновременно — сигнал уверенный.")

    # Limit to 4 items
    return items[:4]


def format_result_caption(
    pair_label: str,
    direction: str,
    expiration_sec: int,
    details: dict,
    outcome: str = "unknown",
) -> str:
    is_buy = direction == "BUY"
    header = (
        f"📈 <b>{html.escape(pair_label)} — ВВЕРХ</b>"
        if is_buy else
        f"📉 <b>{html.escape(pair_label)} — ВНИЗ</b>"
    )

    strategy = details.get("primary_strategy", "")
    mode     = details.get("market_mode", "")
    quality  = details.get("signal_quality", "")
    mode_lbl = _mode_label(mode)
    debug    = details.get("debug", {})
    ind      = debug.get("indicators", {})

    reasons = []

    rsi_val = ind.get("rsi", 50)
    if is_buy and rsi_val < 40:
        reasons.append(f"📊 Рынок слишком сильно упал и готов к отскоку вверх (RSI {rsi_val:.0f})")
    elif not is_buy and rsi_val > 60:
        reasons.append(f"📊 Рынок слишком сильно вырос и готов к откату вниз (RSI {rsi_val:.0f})")

    if strategy == "ema_bounce":
        if is_buy:
            reasons.append("📈 Отскок от скользящей средней — тренд продолжается вверх")
        else:
            reasons.append("📉 Отскок от скользящей средней — тренд продолжается вниз")

    elif strategy == "squeeze_breakout":
        reasons.append("💥 Пробой после сжатия — импульс в вашу сторону")

    elif strategy == "level_bounce":
        reasons.append("🎯 Отскок от важного уровня поддержки/сопротивления")

    elif strategy == "micro_breakout":
        reasons.append("🔓 Пробой ключевого уровня с подтверждением")

    elif strategy == "divergence":
        reasons.append("📐 Дивергенция: цена и индикаторы расходятся — разворот ожидается")

    if mode_lbl:
        reasons.append(f"🏷 Режим рынка: {mode_lbl}")

    reasons_text = "\n".join(reasons) if reasons else "• Большинство факторов сошлись в одном направлении"

    if outcome == "win":
        outcome_line = "✅ <b>Сделка закрыта в плюс!</b>"
    elif outcome == "loss":
        outcome_line = "❌ <b>Сделка закрыта в минус</b>"
    else:
        outcome_line = "📋 Результат — смотри скриншот выше"

    lines = [
        header,
        f"<i>Время сделки: {expiration_sec} сек</i>",
        "",
        outcome_line,
        "",
        "<b>Почему был дан именно этот сигнал:</b>",
        reasons_text,
    ]
    return "\n".join(lines)
