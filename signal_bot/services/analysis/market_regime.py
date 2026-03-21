"""
Market Regime Analysis
Determines: uptrend | downtrend | weak_trend | range | chaotic_noise

chaotic_noise is now only set for genuinely dirty, alternating markets.
range is preferred over chaotic_noise when direction is unclear.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class MarketRegimeResult:
    regime: str           # uptrend | downtrend | weak_trend | range | chaotic_noise
    trend_direction: str  # up | down | sideways
    volatility_state: str # high | normal | compressed
    buy_score: float      # 0-100
    sell_score: float     # 0-100
    explanation: str


def market_regime_analysis(df: pd.DataFrame) -> MarketRegimeResult:
    n = len(df)
    if n < 15:
        # Not enough data → range (neutral), not chaotic
        return MarketRegimeResult(
            "range", "sideways", "normal", 55.0, 55.0,
            "Мало данных — считаем боковым рынком"
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

    # ── Swing structure ───────────────────────────────────────────────────────
    lookback = min(30, n)
    seg = max(3, lookback // 3)
    h = high.iloc[-lookback:]
    l = low.iloc[-lookback:]
    h1, h2, h3 = float(h.iloc[:seg].max()), float(h.iloc[seg:2*seg].max()), float(h.iloc[2*seg:].max())
    l1, l2, l3 = float(l.iloc[:seg].min()), float(l.iloc[seg:2*seg].min()), float(l.iloc[2*seg:].min())

    hh = h3 > h2 > h1
    hl = l3 > l2 > l1
    lh = h3 < h2 < h1
    ll = l3 < l2 < l1

    # ── Direction alternation (chaos indicator) ───────────────────────────────
    # Use last 10 candles; chaotic_noise only if MOST candles alternate
    last_n = min(10, n)
    dirs   = (close.iloc[-last_n:].values > open_.iloc[-last_n:].values).astype(int)
    changes = int(sum(dirs[i] != dirs[i - 1] for i in range(1, len(dirs))))
    # Threshold raised: need ≥75% alternation rate (was 58%)
    chaos_ratio = changes / max(1, last_n - 1)

    # ── Body size (tiny bodies = indecision, not necessarily chaos) ───────────
    bodies     = abs(close.iloc[-last_n:].values - open_.iloc[-last_n:].values)
    ranges_    = (high.iloc[-last_n:].values - low.iloc[-last_n:].values)
    avg_body_r = float(np.mean(bodies / (ranges_ + 1e-10)))  # 0=doji, 1=full body

    # ── Bullish % ────────────────────────────────────────────────────────────
    recent   = min(20, n)
    bull_pct = float((close.iloc[-recent:] > open_.iloc[-recent:]).mean() * 100)

    # ── ATR volatility ────────────────────────────────────────────────────────
    tr       = pd.concat([high - low,
                           (high - close.shift()).abs(),
                           (low  - close.shift()).abs()], axis=1).max(axis=1)
    atr_now  = float(tr.rolling(10).mean().iloc[-1])
    atr_hist = float(tr.rolling(10).mean().iloc[-21:-1].mean()) if n > 31 else atr_now
    atr_r    = atr_now / atr_hist if atr_hist > 0 else 1.0

    if atr_r > 1.5:
        vol_state = "high"
    elif atr_r < 0.6:
        vol_state = "compressed"
    else:
        vol_state = "normal"

    # ── Classify ─────────────────────────────────────────────────────────────
    # CHAOTIC: high alternation rate AND tiny candle bodies
    if chaos_ratio >= 0.75 and avg_body_r < 0.35:
        return MarketRegimeResult(
            "chaotic_noise", "sideways", vol_state, 0.0, 0.0,
            f"Хаотичный рынок: {changes}/{last_n-1} смен, тела свечей малы ({avg_body_r:.2f})"
        )

    # Score for trend directions
    up_pts  = (40 if (hh and hl) else (20 if (hh or hl) else 0))
    up_pts += (25 if ema_slope_pct >  0.02 else (12 if ema_slope_pct >  0.005 else 0))
    up_pts += (20 if bull_pct > 60 else (10 if bull_pct > 52 else 0))

    dn_pts  = (40 if (lh and ll) else (20 if (lh or ll) else 0))
    dn_pts += (25 if ema_slope_pct < -0.02 else (12 if ema_slope_pct < -0.005 else 0))
    dn_pts += (20 if bull_pct < 40 else (10 if bull_pct < 48 else 0))

    # STRONG UPTREND
    if up_pts >= 60 and up_pts > dn_pts + 15:
        return MarketRegimeResult(
            "uptrend", "up", vol_state,
            float(min(100, up_pts)), 35.0,
            f"Восходящий тренд: EMA {ema_slope_pct:+.3f}%, бычьих {bull_pct:.0f}%"
        )

    # STRONG DOWNTREND
    if dn_pts >= 60 and dn_pts > up_pts + 15:
        return MarketRegimeResult(
            "downtrend", "down", vol_state,
            35.0, float(min(100, dn_pts)),
            f"Нисходящий тренд: EMA {ema_slope_pct:+.3f}%, медвежьих {100-bull_pct:.0f}%"
        )

    # WEAK TREND (one side slightly dominant — allow signals by trend)
    if up_pts >= 35 and up_pts > dn_pts + 8:
        return MarketRegimeResult(
            "weak_trend", "up", vol_state,
            65.0, 45.0,
            f"Слабый бычий уклон: EMA {ema_slope_pct:+.3f}%"
        )

    if dn_pts >= 35 and dn_pts > up_pts + 8:
        return MarketRegimeResult(
            "weak_trend", "down", vol_state,
            45.0, 65.0,
            f"Слабый медвежий уклон: EMA {ema_slope_pct:+.3f}%"
        )

    # RANGE (default — most common, allows level_bounce and false_breakout)
    return MarketRegimeResult(
        "range", "sideways", vol_state,
        60.0, 60.0,
        f"Боковой рынок: EMA flat ({ema_slope_pct:+.3f}%)"
    )
