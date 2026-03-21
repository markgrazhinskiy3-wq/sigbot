import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config as _cfg

logger = logging.getLogger(__name__)

CONFIDENCE_THRESHOLD: int = _cfg.SIGNAL_CONFIDENCE_THRESHOLD

# Minimum relative spread for EMA/Momentum to be directional (not NEUTRAL)
# OTC pairs move in tiny increments — keep thresholds small to avoid over-filtering
EMA_NEUTRAL_PCT      = 0.0001   # 0.01% of price — truly flat EMA = NEUTRAL
MOMENTUM_NEUTRAL_PCT = 0.0002   # 0.02% of price — micro-noise = NEUTRAL
# Bollinger Band: inner 40% of band width = ranging market = NEUTRAL
BB_NEUTRAL_ZONE      = 0.20     # price within 20%–80% of band = NEUTRAL


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

    Indicators (5 total, threshold = CONFIDENCE_THRESHOLD, default 4):
      1. RSI(14)          — BUY < 40 | SELL > 60 | NEUTRAL 40–60
      2. EMA cross(9/21)  — BUY/SELL only when spread ≥ 0.05 % of price, else NEUTRAL
      3. Stochastic(5,3,3)— BUY K < 25 | SELL K > 75 | NEUTRAL 25–75
      4. Momentum(10)     — BUY/SELL only when |Δ| ≥ 0.05 % of price, else NEUTRAL
      5. Bollinger(20,2)  — BUY near lower band | SELL near upper band | NEUTRAL middle
    """
    if len(candles) < 21:
        logger.warning("Not enough candles (%d < 21), no signal", len(candles))
        return SignalResult("NO_SIGNAL", 0, {"error": "not enough data"})

    df = pd.DataFrame(candles)
    df = df.rename(columns=str.lower)
    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])

    if len(df) < 21:
        return SignalResult("NO_SIGNAL", 0, {"error": "bad candle data"})

    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    price = float(close.iloc[-1])

    # ── 1. RSI(14) ────────────────────────────────────────────────────────────
    try:
        import pandas_ta as ta
        rsi_series = ta.rsi(close, length=14)
        rsi = float(rsi_series.iloc[-1]) if rsi_series is not None else 50.0
    except Exception:
        rsi = _manual_rsi(close, 14)

    rsi_signal = "BUY" if rsi < 40 else ("SELL" if rsi > 60 else "NEUTRAL")

    # ── 2. EMA cross (9 / 21) — with neutral dead-band ───────────────────────
    try:
        ema_fast = ta.ema(close, length=9)
        ema_slow = ta.ema(close, length=21)
        ema9  = float(ema_fast.iloc[-1])
        ema21 = float(ema_slow.iloc[-1])
    except Exception:
        ema9  = float(close.ewm(span=9,  adjust=False).mean().iloc[-1])
        ema21 = float(close.ewm(span=21, adjust=False).mean().iloc[-1])

    ema_spread = abs(ema9 - ema21) / price if price else 0
    if ema_spread < EMA_NEUTRAL_PCT:
        ema_signal = "NEUTRAL"          # spread too small — no clear trend
    else:
        ema_signal = "BUY" if ema9 > ema21 else "SELL"

    # ── 3. Stochastic (5, 3, 3) ───────────────────────────────────────────────
    try:
        stoch    = ta.stoch(high, low, close, k=5, d=3, smooth_k=3)
        stoch_k  = float(stoch["STOCHk_5_3_3"].iloc[-1])
        stoch_d  = float(stoch["STOCHd_5_3_3"].iloc[-1])
    except Exception:
        stoch_k, stoch_d = _manual_stochastic(high, low, close, 5, 3)

    stoch_signal = (
        "BUY"  if stoch_k < 25
        else ("SELL" if stoch_k > 75
              else "NEUTRAL")
    )

    # ── 4. Momentum(10) — with neutral dead-band ──────────────────────────────
    momentum_period = 10
    if len(close) > momentum_period:
        momentum = float(close.iloc[-1]) - float(close.iloc[-1 - momentum_period])
    else:
        momentum = 0.0

    mom_pct = abs(momentum) / price if price else 0
    if mom_pct < MOMENTUM_NEUTRAL_PCT:
        momentum_signal = "NEUTRAL"     # move too small — no real momentum
    else:
        momentum_signal = "BUY" if momentum > 0 else "SELL"

    # ── 5. Bollinger Bands (20, 2) — replaces raw last-candle direction ───────
    bb_period = 20
    bb_std    = 2.0
    bb_mid    = float(close.rolling(bb_period).mean().iloc[-1])
    bb_std_v  = float(close.rolling(bb_period).std(ddof=0).iloc[-1])
    bb_upper  = bb_mid + bb_std * bb_std_v
    bb_lower  = bb_mid - bb_std * bb_std_v
    bb_width  = bb_upper - bb_lower

    if bb_width == 0:
        bb_signal = "NEUTRAL"
    else:
        # Relative position: 0 = lower band, 0.5 = middle, 1 = upper band
        bb_pos = (price - bb_lower) / bb_width
        neutral_low  = 0.5 - BB_NEUTRAL_ZONE
        neutral_high = 0.5 + BB_NEUTRAL_ZONE
        if bb_pos <= (0.5 - BB_NEUTRAL_ZONE):
            bb_signal = "BUY"           # near or below lower band
        elif bb_pos >= (0.5 + BB_NEUTRAL_ZONE):
            bb_signal = "SELL"          # near or above upper band
        else:
            bb_signal = "NEUTRAL"       # mid-band — ranging market, skip

    # ── Aggregate ─────────────────────────────────────────────────────────────
    signals   = [rsi_signal, ema_signal, stoch_signal, momentum_signal, bb_signal]
    buy_count  = signals.count("BUY")
    sell_count = signals.count("SELL")

    if buy_count > sell_count:
        direction  = "BUY"
        confidence = buy_count
    elif sell_count > buy_count:
        direction  = "SELL"
        confidence = sell_count
    else:
        direction  = "NO_SIGNAL"
        confidence = 0

    if confidence < CONFIDENCE_THRESHOLD:
        direction = "NO_SIGNAL"

    details = {
        "RSI":      {"value": round(rsi, 2),     "signal": rsi_signal},
        "EMA":      {"ema9": round(ema9, 5), "ema21": round(ema21, 5),
                     "spread_pct": round(ema_spread * 100, 4), "signal": ema_signal},
        "Stoch":    {"k": round(stoch_k, 2), "d": round(stoch_d, 2), "signal": stoch_signal},
        "Momentum": {"value": round(momentum, 5), "pct": round(mom_pct * 100, 4),
                     "signal": momentum_signal},
        "BB":       {"upper": round(bb_upper, 5), "mid": round(bb_mid, 5),
                     "lower": round(bb_lower, 5), "pos": round(bb_pos if bb_width else 0.5, 3),
                     "signal": bb_signal},
        "buy_count":  buy_count,
        "sell_count": sell_count,
    }
    logger.info("Signal: %s (conf=%d) | %s", direction, confidence, details)
    return SignalResult(direction=direction, confidence=confidence, details=details)


# ── Fallback manual implementations ────────────────────────────────────────

def _manual_rsi(close: pd.Series, period: int = 14) -> float:
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs  = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if not np.isnan(rsi.iloc[-1]) else 50.0


def _manual_stochastic(
    high: pd.Series, low: pd.Series, close: pd.Series, k_period: int, d_period: int
) -> tuple[float, float]:
    lowest_low    = low.rolling(k_period).min()
    highest_high  = high.rolling(k_period).max()
    denom         = (highest_high - lowest_low).replace(0, np.nan)
    stoch_k       = 100 * (close - lowest_low) / denom
    stoch_d       = stoch_k.rolling(d_period).mean()
    return (
        float(stoch_k.iloc[-1]) if not np.isnan(stoch_k.iloc[-1]) else 50.0,
        float(stoch_d.iloc[-1]) if not np.isnan(stoch_d.iloc[-1]) else 50.0,
    )
