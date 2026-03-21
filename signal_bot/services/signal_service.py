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


def format_signal_message(signal: SignalResponse) -> str:
    if signal.direction == "NO_SIGNAL":
        return (
            f"🔍 <b>{signal.pair}</b>\n\n"
            f"⚠️ <b>Сигнал слабый</b>\n"
            f"Рынок не даёт чёткого направления. Воздержитесь от сделки.\n\n"
            f"<i>Попробуйте другую пару или подождите.</i>"
        )

    arrow = "🟢" if signal.direction == "BUY" else "🔴"

    lines = [
        f"{arrow} <b>{signal.pair}</b>\n",
        f"📊 Сигнал: <b>{signal.direction}</b>",
        f"💪 Сила: {signal.confidence}/5",
        f"⏱ Экспирация: {signal.expiration_sec} сек",
        f"\n<i>Анализ и результат придут по истечении сделки.</i>",
    ]
    return "\n".join(lines)


def format_result_caption(
    pair_label: str,
    direction: str,
    expiration_sec: int,
    details: dict,
    outcome: str = "unknown",
) -> str:
    arrow = "🟢" if direction == "BUY" else "🔴"
    d = details

    rsi = d.get("RSI", {})
    ema = d.get("EMA", {})
    stoch = d.get("Stoch", {})
    momentum = d.get("Momentum", {})
    candle = d.get("Candle", {})

    reasons = []

    rsi_val = rsi.get("value", "?")
    if rsi.get("signal") == direction:
        hint = " — перепроданность" if direction == "BUY" else " — перекупленность"
        reasons.append(f"• RSI ({rsi_val}){hint}")

    if ema.get("signal") == direction:
        hint = " — восходящий кросс" if direction == "BUY" else " — нисходящий кросс"
        reasons.append(f"• EMA 9/21{hint}")

    stoch_k = stoch.get("k", "?")
    if stoch.get("signal") == direction:
        hint = " — зона перепроданности" if direction == "BUY" else " — зона перекупленности"
        reasons.append(f"• Stoch K ({stoch_k}){hint}")

    if momentum.get("signal") == direction:
        hint = " — положительный импульс" if direction == "BUY" else " — отрицательный импульс"
        reasons.append(f"• Momentum{hint}")

    if candle.get("signal") == direction:
        hint = " — бычья свеча" if direction == "BUY" else " — медвежья свеча"
        reasons.append(f"• Свеча{hint}")

    reasons_text = "\n".join(reasons) if reasons else "• Большинство индикаторов совпали"

    if outcome == "win":
        outcome_line = "✅ <b>+  Сделка выиграна!</b>"
    elif outcome == "loss":
        outcome_line = "❌ <b>−  Сделка проиграна</b>"
    else:
        outcome_line = "📊 Результат — смотри скриншот"

    lines = [
        f"{arrow} <b>{pair_label}</b>",
        f"Сигнал: <b>{direction}</b> | Экспирация: {expiration_sec} сек\n",
        outcome_line,
        f"\n<b>Почему вошли в {direction}:</b>",
        reasons_text,
    ]
    return "\n".join(lines)
