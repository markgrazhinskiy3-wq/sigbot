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
    """
    Full orchestration: fetch candles → run strategy → return signal.
    """
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
            f"Индикаторы не дают чёткого направления. Воздержитесь от сделки.\n\n"
            f"<i>Попробуйте другую пару или подождите.</i>"
        )

    arrow = "🟢" if signal.direction == "BUY" else "🔴"
    d = signal.details

    rsi = d.get("RSI", {})
    ema = d.get("EMA", {})
    stoch = d.get("Stoch", {})
    momentum = d.get("Momentum", {})
    candle = d.get("Candle", {})

    def sig_emoji(s: str) -> str:
        return "↑" if s == "BUY" else ("↓" if s == "SELL" else "–")

    lines = [
        f"{arrow} <b>{signal.pair}</b>\n",
        f"📊 Сигнал: <b>{signal.direction}</b>",
        f"💪 Сила: {signal.confidence}/5",
        f"⏱ Экспирация: {signal.expiration_sec} сек\n",
        f"<b>Индикаторы:</b>",
        f"• RSI({rsi.get('value', '?')}): {sig_emoji(rsi.get('signal', ''))} {rsi.get('signal', '?')}",
        f"• EMA 9/21: {sig_emoji(ema.get('signal', ''))} {ema.get('signal', '?')}",
        f"• Stoch K={stoch.get('k', '?')}: {sig_emoji(stoch.get('signal', ''))} {stoch.get('signal', '?')}",
        f"• Momentum: {sig_emoji(momentum.get('signal', ''))} {momentum.get('signal', '?')}",
        f"• Свеча: {sig_emoji(candle.get('signal', ''))} {candle.get('signal', '?')}",
        f"\n<i>Скриншот результата придёт по истечении времени.</i>",
    ]
    return "\n".join(lines)
