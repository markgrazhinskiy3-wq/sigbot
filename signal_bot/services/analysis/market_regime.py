"""
Market Regime Analysis — v7
Determines: uptrend | downtrend | weak_trend | range | chaotic_noise

chaotic_noise = ATR spike + no clear EMA direction + price whipping both sides.
It is a soft penalty category (−15), not a hard block.
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
        return MarketRegimeResult(
            "range", "sideways", "normal", 55.0, 55.0,
            "Мало данных — считаем боковым рынком"
        )

    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    open_ = df["open"]

    # ── EMA(9) vs EMA(21) slope and cross ─────────────────────────────────────
    ema9  = close.ewm(span=9,  adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()

    ema9_now  = float(ema9.iloc[-1])
    ema21_now = float(ema21.iloc[-1])
    price_now = float(close.iloc[-1])

    # EMA9 slope over last min(8, n//3) bars
    slope_bars = max(3, min(8, n // 3))
    ema9_base  = float(ema9.iloc[-1 - slope_bars])
    ema9_slope = (ema9_now - ema9_base) / ema9_base * 100 if ema9_base != 0 else 0.0

    # EMA9 vs EMA21 spread (normalised to price)
    ema_spread_pct = (ema9_now - ema21_now) / price_now * 100 if price_now else 0.0

    # ── Swing structure (last 30 bars) ────────────────────────────────────────
    lookback = min(30, n)
    seg = max(3, lookback // 3)
    h = high.iloc[-lookback:]
    l = low.iloc[-lookback:]
    h1 = float(h.iloc[:seg].max());      h2 = float(h.iloc[seg:2*seg].max()); h3 = float(h.iloc[2*seg:].max())
    l1 = float(l.iloc[:seg].min());      l2 = float(l.iloc[seg:2*seg].min()); l3 = float(l.iloc[2*seg:].min())

    hh = h3 > h2 > h1   # higher highs
    hl = l3 > l2 > l1   # higher lows
    lh = h3 < h2 < h1   # lower highs
    ll = l3 < l2 < l1   # lower lows

    # ── Bullish % of recent 20 candles ────────────────────────────────────────
    recent   = min(20, n)
    bull_pct = float((close.iloc[-recent:] > open_.iloc[-recent:]).mean() * 100)

    # ── ATR volatility ────────────────────────────────────────────────────────
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)

    atr_now  = float(tr.rolling(14).mean().iloc[-1])
    atr_hist = float(tr.rolling(14).mean().iloc[-21:-1].mean()) if n > 35 else atr_now
    atr_r    = atr_now / atr_hist if atr_hist > 0 else 1.0

    if atr_r > 1.5:
        vol_state = "high"
    elif atr_r < 0.5:
        vol_state = "compressed"
    else:
        vol_state = "normal"

    # ── Direction alternation (chaos indicator) ────────────────────────────────
    last_n = min(10, n)
    dirs   = (close.iloc[-last_n:].values > open_.iloc[-last_n:].values).astype(int)
    changes = int(sum(dirs[i] != dirs[i - 1] for i in range(1, len(dirs))))
    chaos_ratio = changes / max(1, last_n - 1)

    # Tiny body ratio (last 10 bars)
    bodies_  = abs(close.iloc[-last_n:].values - open_.iloc[-last_n:].values)
    ranges_  = (high.iloc[-last_n:].values - low.iloc[-last_n:].values) + 1e-10
    avg_body_r = float(np.mean(bodies_ / ranges_))

    # ── CHAOTIC NOISE: ATR spike + no clear EMA direction + price whipping ────
    # Requires ALL three: ATR elevated, EMA direction unclear, high alternation
    ema_unclear    = abs(ema_spread_pct) < 0.03 and abs(ema9_slope) < 0.05
    price_whipping = chaos_ratio >= 0.70 and avg_body_r < 0.35
    atr_spike      = atr_r >= 1.3

    if ema_unclear and price_whipping and atr_spike:
        return MarketRegimeResult(
            "chaotic_noise", "sideways", vol_state, 20.0, 20.0,
            f"Хаотичный рынок: ATR×{atr_r:.2f}, смен {changes}/{last_n-1}, EMA нейтрально"
        )

    # ── Score for trend classification ────────────────────────────────────────
    up_pts = 0
    up_pts += (40 if (hh and hl) else (20 if (hh or hl) else 0))
    up_pts += (25 if ema9_slope > 0.03 else (12 if ema9_slope > 0.008 else 0))
    up_pts += (15 if ema_spread_pct > 0.03 else (7 if ema_spread_pct > 0.008 else 0))
    up_pts += (20 if bull_pct > 60 else (10 if bull_pct > 52 else 0))

    dn_pts = 0
    dn_pts += (40 if (lh and ll) else (20 if (lh or ll) else 0))
    dn_pts += (25 if ema9_slope < -0.03 else (12 if ema9_slope < -0.008 else 0))
    dn_pts += (15 if ema_spread_pct < -0.03 else (7 if ema_spread_pct < -0.008 else 0))
    dn_pts += (20 if bull_pct < 40 else (10 if bull_pct < 48 else 0))

    # ── Classify ─────────────────────────────────────────────────────────────
    if up_pts >= 60 and up_pts > dn_pts + 15:
        return MarketRegimeResult(
            "uptrend", "up", vol_state,
            float(min(100, up_pts)), 35.0,
            f"Восходящий тренд: EMA9↑{ema9_slope:+.3f}%, spread={ema_spread_pct:+.3f}%, бычьих {bull_pct:.0f}%"
        )

    if dn_pts >= 60 and dn_pts > up_pts + 15:
        return MarketRegimeResult(
            "downtrend", "down", vol_state,
            35.0, float(min(100, dn_pts)),
            f"Нисходящий тренд: EMA9↓{ema9_slope:+.3f}%, spread={ema_spread_pct:+.3f}%, медвежьих {100-bull_pct:.0f}%"
        )

    if up_pts >= 35 and up_pts > dn_pts + 8:
        return MarketRegimeResult(
            "weak_trend", "up", vol_state,
            65.0, 45.0,
            f"Слабый бычий уклон: EMA9 {ema9_slope:+.3f}%"
        )

    if dn_pts >= 35 and dn_pts > up_pts + 8:
        return MarketRegimeResult(
            "weak_trend", "down", vol_state,
            45.0, 65.0,
            f"Слабый медвежий уклон: EMA9 {ema9_slope:+.3f}%"
        )

    return MarketRegimeResult(
        "range", "sideways", vol_state,
        60.0, 60.0,
        f"Боковой рынок: EMA9 flat ({ema9_slope:+.3f}%), spread={ema_spread_pct:+.3f}%"
    )
