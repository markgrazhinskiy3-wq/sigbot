"""
Market Regime Analysis
Determines: uptrend | downtrend | range | chaotic_noise
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class MarketRegimeResult:
    regime: str           # uptrend | downtrend | range | chaotic_noise
    trend_direction: str  # up | down | sideways
    volatility_state: str # high | normal | compressed
    buy_score: float      # 0-100: how favorable this regime is for BUY
    sell_score: float     # 0-100: how favorable for SELL
    explanation: str


def market_regime_analysis(df: pd.DataFrame) -> MarketRegimeResult:
    n = len(df)
    if n < 20:
        return MarketRegimeResult(
            "chaotic_noise", "sideways", "low", 0.0, 0.0,
            "Недостаточно данных для анализа режима"
        )

    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    open_ = df["open"]

    # ── EMA slope ────────────────────────────────────────────────────────────
    ema20 = close.ewm(span=20, adjust=False).mean()
    slope_bars = min(10, n // 3)
    base = float(ema20.iloc[-1 - slope_bars])
    ema_slope_pct = (float(ema20.iloc[-1]) - base) / base * 100 if base != 0 else 0.0

    # ── Swing structure (split candles into 3 segments) ───────────────────────
    lookback = min(30, n)
    seg = max(3, lookback // 3)
    h = high.iloc[-lookback:]
    l = low.iloc[-lookback:]
    h1 = float(h.iloc[:seg].max())
    h2 = float(h.iloc[seg:2 * seg].max())
    h3 = float(h.iloc[2 * seg:].max())
    l1 = float(l.iloc[:seg].min())
    l2 = float(l.iloc[seg:2 * seg].min())
    l3 = float(l.iloc[2 * seg:].min())

    hh = h3 > h2 > h1   # higher highs → uptrend
    hl = l3 > l2 > l1   # higher lows  → uptrend
    lh = h3 < h2 < h1   # lower highs  → downtrend
    ll = l3 < l2 < l1   # lower lows   → downtrend

    # ── Candle direction chaos ────────────────────────────────────────────────
    last_n = min(12, n)
    dirs = (close.iloc[-last_n:].values > open_.iloc[-last_n:].values).astype(int)
    changes = int(sum(dirs[i] != dirs[i - 1] for i in range(1, len(dirs))))
    chaos_ratio = changes / max(1, last_n - 1)  # 0-1

    # ── Bullish candle percentage (last 20) ───────────────────────────────────
    recent = min(20, n)
    bull_pct = float((close.iloc[-recent:] > open_.iloc[-recent:]).mean() * 100)

    # ── Volatility via ATR ────────────────────────────────────────────────────
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)
    atr_now = float(tr.rolling(10).mean().iloc[-1])
    atr_avg = float(tr.rolling(10).mean().iloc[-21:-1].mean()) if n > 31 else atr_now
    atr_ratio = atr_now / atr_avg if atr_avg > 0 else 1.0

    if atr_ratio > 1.5:
        volatility_state = "high"
    elif atr_ratio < 0.6:
        volatility_state = "compressed"
    else:
        volatility_state = "normal"

    # ── Classify regime ───────────────────────────────────────────────────────
    if chaos_ratio >= 0.58:
        return MarketRegimeResult(
            "chaotic_noise", "sideways", volatility_state,
            0.0, 0.0,
            f"Хаотичный рынок: {changes} смен направления из {last_n - 1} свечей"
        )

    up_pts  = (40 if (hh and hl) else (20 if (hh or hl) else 0))
    up_pts += (30 if ema_slope_pct >  0.02 else (15 if ema_slope_pct >  0.005 else 0))
    up_pts += (20 if bull_pct > 60 else (10 if bull_pct > 50 else 0))

    dn_pts  = (40 if (lh and ll) else (20 if (lh or ll) else 0))
    dn_pts += (30 if ema_slope_pct < -0.02 else (15 if ema_slope_pct < -0.005 else 0))
    dn_pts += (20 if bull_pct < 40 else (10 if bull_pct < 50 else 0))

    if up_pts >= 40 and up_pts > dn_pts + 10:
        return MarketRegimeResult(
            "uptrend", "up", volatility_state,
            float(min(100, up_pts)), 30.0,
            f"Восходящий тренд: EMA {ema_slope_pct:+.3f}%, бычьих {bull_pct:.0f}%"
        )

    if dn_pts >= 40 and dn_pts > up_pts + 10:
        return MarketRegimeResult(
            "downtrend", "down", volatility_state,
            30.0, float(min(100, dn_pts)),
            f"Нисходящий тренд: EMA {ema_slope_pct:+.3f}%, медвежьих {100 - bull_pct:.0f}%"
        )

    return MarketRegimeResult(
        "range", "sideways", volatility_state,
        55.0, 55.0,
        f"Боковой рынок: нет чёткой структуры H/L"
    )
