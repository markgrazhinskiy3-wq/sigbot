import logging
from dataclasses import dataclass

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from services.candle_cache import get_candles_cached as get_candles, get_cached_symbols
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


async def scan_best_signal(expiration_sec: int, pairs_map: dict[str, str]) -> SignalResponse | None:
    """
    Scan all cached pairs and return the best BUY/SELL signal (highest confidence).
    `pairs_map` is {symbol: label}. Returns None if no signal found on any pair.
    """
    symbols = get_cached_symbols()
    if not symbols:
        return None

    best: SignalResponse | None = None
    for symbol in symbols:
        label = pairs_map.get(symbol, symbol)
        candles = await get_candles(symbol, count=80)
        if not candles:
            continue
        result: SignalResult = calculate_signal(candles)
        if result.direction not in ("BUY", "SELL"):
            continue
        resp = SignalResponse(
            direction=result.direction,
            confidence=result.confidence,
            details=result.details,
            pair=label,
            expiration_sec=expiration_sec,
            symbol=symbol,
        )
        if best is None or result.confidence > best.confidence:
            best = resp
    return best


async def get_signal(symbol: str, pair_label: str, expiration_sec: int) -> SignalResponse:
    logger.info("Fetching signal for %s (exp=%ds)", symbol, expiration_sec)
    candles = await get_candles(symbol, count=80)   # more candles for level detection
    result: SignalResult = calculate_signal(candles)
    return SignalResponse(
        direction=result.direction,
        confidence=result.confidence,
        details=result.details,
        pair=pair_label,
        expiration_sec=expiration_sec,
    )


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
    return "низкая"


def format_signal_message(signal: SignalResponse) -> str:
    if signal.direction == "NO_SIGNAL":
        d      = signal.details if isinstance(signal.details, dict) else {}
        reject = d.get("reject_reason") or ""
        regime = d.get("regime", "")
        hard   = d.get("hard_conflicts", [])
        regime_label = {
            "chaotic_noise": "🌪 Хаотичный рынок",
            "range":         "↔️ Боковой рынок",
            "uptrend":       "📈 Восходящий тренд",
            "downtrend":     "📉 Нисходящий тренд",
            "weak_trend":    "〰️ Слабый тренд",
        }.get(regime, "")

        lines = [
            f"🔍 <b>{signal.pair}</b>",
            "",
            f"⚠️ <b>Сигнал не найден</b>" + (f" — {regime_label}" if regime_label else ""),
            "Условия входа не выполнены — лучше пропустить.",
        ]
        if hard:
            lines.append(f"\n<i>🚫 {hard[0]}</i>")
        elif reject:
            lines.append(f"\n<i>{reject}</i>")
        lines.append("")
        lines.append("<i>Попробуйте другую пару или подождите немного.</i>")
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
    explanation_lines = _build_explanation(signal.direction, d)

    lines = [
        f"{arrow} <b>{signal.pair}</b>",
        "",
        f"📊 Сигнал: <b>{dir_label}</b>",
        f"💪 Уверенность: {bar} {signal.confidence}/5 ({label})",
        "",
    ]

    if explanation_lines:
        lines.append("<b>Почему:</b>")
        for item in explanation_lines:
            lines.append(f"• {item}")
        lines.append("")

    lines.append("<i>Откройте сделку вручную на Pocket Option.</i>")
    return "\n".join(lines)


def _build_explanation(direction: str, details: dict) -> list[str]:
    """
    Build 2–4 bullet points in plain Russian language explaining the signal.
    No technical jargon — as if explaining to a friend who has never traded.
    """
    is_buy   = direction == "BUY"
    primary  = details.get("primary_strategy") or ""
    regime   = details.get("regime", "")
    quality  = details.get("signal_quality", "")
    debug    = details.get("debug", {})
    modules  = debug.get("modules", {})

    items: list[str] = []

    # ── 1. Main reason — what triggered the signal ────────────────────────────
    if primary == "impulse":
        if is_buy:
            items.append(
                "Рынок шёл вверх, затем сделал небольшой откат — это нормальная пауза перед продолжением роста."
            )
        else:
            items.append(
                "Рынок двигался вниз, немного скорректировался вверх — и теперь снова разворачивается в сторону падения."
            )

    elif primary == "bounce":
        if is_buy:
            items.append(
                "Цена опустилась до уровня, где раньше покупатели уже останавливали падение. Сейчас они снова вступают в игру."
            )
        else:
            items.append(
                "Цена поднялась до уровня, где раньше продавцы всегда останавливали рост. Покупатели выдохлись — ожидаем разворот."
            )

    elif primary == "breakout":
        if is_buy:
            items.append(
                "Цена ненадолго ушла ниже важного уровня, но не смогла там закрепиться и быстро вернулась. Продавцы попали в ловушку — ожидаем движение вверх."
            )
        else:
            items.append(
                "Цена пробилась выше важного уровня, но не удержалась там и упала обратно. Покупатели попали в ловушку — ожидаем движение вниз."
            )

    else:
        # Fallback — generic by regime
        if is_buy:
            items.append("Большинство факторов указывают на движение вверх.")
        else:
            items.append("Большинство факторов указывают на движение вниз.")

    # ── 2. Market regime context ──────────────────────────────────────────────
    if regime == "uptrend" and is_buy:
        items.append("Общий тренд сейчас восходящий — работаем по движению.")
    elif regime == "downtrend" and not is_buy:
        items.append("Общий тренд сейчас нисходящий — работаем по движению.")
    elif regime == "uptrend" and not is_buy:
        items.append("Тренд восходящий, но цена явно устала расти — разворот назрел.")
    elif regime == "downtrend" and is_buy:
        items.append("Тренд нисходящий, но цена чрезмерно упала — ожидаем кратковременный отскок.")
    elif regime == "range":
        if is_buy:
            items.append("Рынок движется в боковом диапазоне — торгуем от нижней границы.")
        else:
            items.append("Рынок движется в боковом диапазоне — торгуем от верхней границы.")
    elif regime == "weak_trend":
        items.append("Тренд слабый — сделка короткая, быстрый вход и выход.")

    # ── 3. Supporting factors ─────────────────────────────────────────────────
    candle_buy  = modules.get("candle_strength", {}).get("buy",  0)
    candle_sell = modules.get("candle_strength", {}).get("sell", 0)
    ind_buy     = modules.get("indicators", {}).get("buy",  0)
    ind_sell    = modules.get("indicators", {}).get("sell", 0)

    if is_buy and candle_buy >= 55:
        items.append("Последние свечи закрываются с ростом — покупатели активны.")
    elif not is_buy and candle_sell >= 55:
        items.append("Последние свечи закрываются в минус — продавцы активны.")

    if is_buy and ind_buy >= 45:
        items.append("Рыночные показатели дополнительно подтверждают рост.")
    elif not is_buy and ind_sell >= 45:
        items.append("Рыночные показатели дополнительно подтверждают падение.")

    # ── 4. Confidence note ────────────────────────────────────────────────────
    if quality == "strong":
        items.append("Несколько факторов совпали одновременно — сигнал уверенный.")

    return items


def format_result_caption(
    pair_label: str,
    direction: str,
    expiration_sec: int,
    details: dict,
    outcome: str = "unknown",
) -> str:
    is_buy = direction == "BUY"
    header = (
        f"📈 <b>{pair_label} — ВВЕРХ</b>"
        if is_buy else
        f"📉 <b>{pair_label} — ВНИЗ</b>"
    )
    d = details

    rsi      = d.get("RSI", {})
    ema      = d.get("EMA", {})
    stoch    = d.get("Stoch", {})
    momentum = d.get("Momentum", {})
    bb       = d.get("BB", {})

    reasons = []

    # RSI — объясняем человеческим языком
    rsi_val = rsi.get("value", 50)
    if rsi.get("signal") == direction:
        if is_buy:
            reasons.append(f"📊 Рынок слишком сильно упал и готов к отскоку вверх (RSI {round(rsi_val)})")
        else:
            reasons.append(f"📊 Рынок слишком сильно вырос и готов к откату вниз (RSI {round(rsi_val)})")

    # EMA
    if ema.get("signal") == direction:
        if is_buy:
            reasons.append("📈 Краткосрочный тренд переломился и пошёл вверх")
        else:
            reasons.append("📉 Краткосрочный тренд переломился и пошёл вниз")

    # Stochastic
    stoch_k = stoch.get("k", 50)
    if stoch.get("signal") == direction:
        if is_buy:
            reasons.append(f"🔻 Цена на дне диапазона — продавцы выдохлись (Stoch {round(stoch_k)})")
        else:
            reasons.append(f"🔺 Цена на пике диапазона — покупатели выдохлись (Stoch {round(stoch_k)})")

    # Momentum
    if momentum.get("signal") == direction:
        if is_buy:
            reasons.append("⚡ Движение вверх набирает силу")
        else:
            reasons.append("⚡ Движение вниз набирает силу")

    # Bollinger Bands
    if bb.get("signal") == direction:
        if is_buy:
            reasons.append("📐 Цена вышла за нижнюю границу нормального диапазона — ожидаем возврат вверх")
        else:
            reasons.append("📐 Цена вышла за верхнюю границу нормального диапазона — ожидаем возврат вниз")

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
