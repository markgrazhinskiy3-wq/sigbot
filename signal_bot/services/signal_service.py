import logging
from dataclasses import dataclass

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from services.pocket_browser import get_candles
from services.strategy_engine import calculate_signal, SignalResult

logger = logging.getLogger(__name__)


@dataclass
class SignalResponse:
    direction: str       # "BUY" | "SELL" | "NO_SIGNAL"
    confidence: int
    details: dict
    pair: str
    expiration_sec: int


async def get_signal(symbol: str, pair_label: str, expiration_sec: int) -> SignalResponse:
    logger.info("Fetching signal for %s (exp=%ds)", symbol, expiration_sec)
    candles = await get_candles(symbol, count=60)
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
        return (
            f"🔍 <b>{signal.pair}</b>\n\n"
            f"⚠️ <b>Рынок сейчас в неопределённости</b>\n"
            f"Нет чёткого направления — входить рискованно.\n\n"
            f"<i>Попробуйте другую пару или подождите немного.</i>"
        )

    if signal.direction == "BUY":
        arrow     = "⬆️"
        dir_label = "BUY ⬆️ (ВВЕРХ)"
    else:
        arrow     = "⬇️"
        dir_label = "SELL ⬇️ (ВНИЗ)"

    bar   = _conf_bar(signal.confidence)
    label = _conf_label(signal.confidence)

    return (
        f"{arrow} <b>{signal.pair}</b>\n"
        f"\n"
        f"📊 Сигнал: <b>{dir_label}</b>\n"
        f"💪 Уверенность: {bar} {signal.confidence}/5 ({label})\n"
        f"\n"
        f"<i>Откройте сделку вручную на Pocket Option.</i>"
    )


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
