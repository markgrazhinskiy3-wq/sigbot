import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass

logger = logging.getLogger(__name__)

CONFIDENCE_THRESHOLD = 3


@dataclass
class SignalResult:
    direction: str       # "BUY" | "SELL" | "NO_SIGNAL"
    confidence: int      # number of aligned indicators (0..5)
    details: dict        # per-indicator breakdown


def calculate_signal(candles: list[dict]) -> SignalResult:
    """
    Receives a list of OHLC dicts:
        [{"open": float, "high": float, "low": float, "close": float}, ...]
    Returns a SignalResult.
    """
    if len(candles) < 22:
        logger.warning("Not enough candles (%d < 22), no signal", len(candles))
        return SignalResult("NO_SIGNAL", 0, {"error": "not enough data"})

    df = pd.DataFrame(candles)
    df = df.rename(columns=str.lower)
    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])

    if len(df) < 22:
        return SignalResult("NO_SIGNAL", 0, {"error": "bad candle data"})

    close = df["close"]
    high = df["high"]
    low = df["low"]

    # ── RSI(14) ─────────────────────────────────────────────────────────────
    try:
        import pandas_ta as ta
        rsi_series = ta.rsi(close, length=14)
        rsi = float(rsi_series.iloc[-1]) if rsi_series is not None else 50.0
    except Exception:
        rsi = _manual_rsi(close, 14)

    rsi_signal = "BUY" if rsi < 40 else ("SELL" if rsi > 60 else "NEUTRAL")

    # ── EMA cross (9 / 21) ───────────────────────────────────────────────────
    try:
        ema_fast = ta.ema(close, length=9)
        ema_slow = ta.ema(close, length=21)
        ema9 = float(ema_fast.iloc[-1])
        ema21 = float(ema_slow.iloc[-1])
    except Exception:
        ema9 = float(close.ewm(span=9, adjust=False).mean().iloc[-1])
        ema21 = float(close.ewm(span=21, adjust=False).mean().iloc[-1])

    ema_signal = "BUY" if ema9 > ema21 else "SELL"

    # ── Stochastic (5, 3, 3) ─────────────────────────────────────────────────
    try:
        stoch = ta.stoch(high, low, close, k=5, d=3, smooth_k=3)
        stoch_k = float(stoch[f"STOCHk_5_3_3"].iloc[-1])
        stoch_d = float(stoch[f"STOCHd_5_3_3"].iloc[-1])
    except Exception:
        stoch_k, stoch_d = _manual_stochastic(high, low, close, 5, 3)

    stoch_signal = (
        "BUY" if (stoch_k < 20 and stoch_k > stoch_d)
        else ("SELL" if (stoch_k > 80 and stoch_k < stoch_d)
              else "NEUTRAL")
    )

    # ── Momentum ────────────────────────────────────────────────────────────
    momentum_period = 10
    if len(close) > momentum_period:
        momentum = float(close.iloc[-1]) - float(close.iloc[-1 - momentum_period])
    else:
        momentum = 0.0
    momentum_signal = "BUY" if momentum > 0 else ("SELL" if momentum < 0 else "NEUTRAL")

    # ── Last candle direction ────────────────────────────────────────────────
    last = df.iloc[-1]
    candle_signal = "BUY" if last["close"] > last["open"] else "SELL"

    # ── Aggregate ────────────────────────────────────────────────────────────
    signals = [rsi_signal, ema_signal, stoch_signal, momentum_signal, candle_signal]
    buy_count = signals.count("BUY")
    sell_count = signals.count("SELL")

    if buy_count > sell_count:
        direction = "BUY"
        confidence = buy_count
    elif sell_count > buy_count:
        direction = "SELL"
        confidence = sell_count
    else:
        direction = "NO_SIGNAL"
        confidence = 0

    if confidence < CONFIDENCE_THRESHOLD:
        direction = "NO_SIGNAL"

    details = {
        "RSI": {"value": round(rsi, 2), "signal": rsi_signal},
        "EMA": {"ema9": round(ema9, 5), "ema21": round(ema21, 5), "signal": ema_signal},
        "Stoch": {"k": round(stoch_k, 2), "d": round(stoch_d, 2), "signal": stoch_signal},
        "Momentum": {"value": round(momentum, 5), "signal": momentum_signal},
        "Candle": {"signal": candle_signal},
        "buy_count": buy_count,
        "sell_count": sell_count,
    }
    logger.info("Signal: %s (conf=%d) | %s", direction, confidence, details)
    return SignalResult(direction=direction, confidence=confidence, details=details)


# ── Fallback manual implementations ────────────────────────────────────────

def _manual_rsi(close: pd.Series, period: int = 14) -> float:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if not np.isnan(rsi.iloc[-1]) else 50.0


def _manual_stochastic(
    high: pd.Series, low: pd.Series, close: pd.Series, k_period: int, d_period: int
) -> tuple[float, float]:
    lowest_low = low.rolling(k_period).min()
    highest_high = high.rolling(k_period).max()
    denom = (highest_high - lowest_low).replace(0, np.nan)
    stoch_k = 100 * (close - lowest_low) / denom
    stoch_d = stoch_k.rolling(d_period).mean()
    return (
        float(stoch_k.iloc[-1]) if not np.isnan(stoch_k.iloc[-1]) else 50.0,
        float(stoch_d.iloc[-1]) if not np.isnan(stoch_d.iloc[-1]) else 50.0,
    )
