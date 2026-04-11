"""
Indicator Calculator — fast periods tuned for 1-minute binary options.

Periods:
  EMA(5), EMA(13), EMA(21)
  RSI(7)
  Stochastic(5, 3, 3)
  Momentum(5)
  ATR(10)
  Bollinger Bands(15, 2.0)
  ADX(14)               ← NEW: trend strength filter
  RSI divergence        ← NEW: bull/bear divergence detection
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass, field


@dataclass
class Indicators:
    # EMAs
    ema5:  float
    ema13: float
    ema21: float
    ema5_series:  pd.Series
    ema13_series: pd.Series
    ema21_series: pd.Series

    # RSI(7)
    rsi: float          # current RSI (last candle)
    rsi_prev: float     # RSI of second-to-last candle (real, not stub)
    rsi_series: pd.Series  # full RSI series (last N bars) — for divergence

    # RSI Divergence flags (checked over last 5 bars)
    rsi_bull_div: bool  # price lower low but RSI higher low → bullish reversal
    rsi_bear_div: bool  # price higher high but RSI lower high → bearish reversal

    # Stochastic(5,3,3)
    stoch_k: float      # smoothed %K
    stoch_d: float      # %D
    stoch_k_prev: float

    # Momentum(5)
    momentum: float     # price[now] - price[now-5]
    momentum_prev: float

    # ATR(10)
    atr: float
    atr_avg30: float    # average ATR over last 30 bars (historical baseline)
    atr_ratio: float    # atr / atr_avg30

    # Bollinger Bands(15, 2.0)
    bb_upper: float
    bb_lower: float
    bb_mid:   float
    bb_bw:    float     # bandwidth = (upper - lower) / mid
    bb_bw_prev: float   # bandwidth 5 bars ago (for squeeze detection)

    # ADX(14) — Wilder's Average Directional Index
    adx: float          # 0-100: <20 = weak/no trend, 20-25 = emerging, >25 = strong trend
    adx_plus_di: float  # +DI (bullish directional strength)
    adx_minus_di: float # -DI (bearish directional strength)


def calculate_indicators(df: pd.DataFrame) -> Indicators:
    """Calculate all indicators from a 1-min OHLC DataFrame."""
    n = len(df)
    close = df["close"]
    high  = df["high"]
    low   = df["low"]

    # ── EMAs ─────────────────────────────────────────────────────────────────
    ema5_s  = close.ewm(span=5,  adjust=False).mean()
    ema13_s = close.ewm(span=13, adjust=False).mean()
    ema21_s = close.ewm(span=21, adjust=False).mean()

    # ── RSI(7) + full series ──────────────────────────────────────────────────
    rsi_s        = _rsi_series(close, 7)
    rsi_val      = float(rsi_s.iloc[-1])
    rsi_prev_val = float(rsi_s.iloc[-2]) if n >= 2 and len(rsi_s) >= 2 else rsi_val

    # ── RSI Divergence ────────────────────────────────────────────────────────
    # Look back 5 bars for a simple but reliable divergence signal:
    #   Bullish: current close LOWER than 5 bars ago, but RSI HIGHER → hidden strength
    #   Bearish: current close HIGHER than 5 bars ago, but RSI LOWER → hidden weakness
    # Require a meaningful RSI gap (≥3 pts) to avoid noise
    rsi_bull_div = False
    rsi_bear_div = False
    _DIV_LOOKBACK = 5
    _DIV_RSI_MIN  = 3.0   # minimum RSI delta to count as divergence
    _DIV_PRICE_MIN_PCT = 0.0002  # 0.02% — minimum price move to call it a move

    if n >= _DIV_LOOKBACK + 2 and len(rsi_s) >= _DIV_LOOKBACK + 1:
        price_now  = float(close.iloc[-1])
        price_past = float(close.iloc[-1 - _DIV_LOOKBACK])
        rsi_now    = rsi_val
        rsi_past   = float(rsi_s.iloc[-1 - _DIV_LOOKBACK])

        price_move = (price_now - price_past) / price_past if price_past != 0 else 0.0
        rsi_delta  = rsi_now - rsi_past

        # Bullish: price lower (negative move), RSI higher (positive delta)
        if price_move < -_DIV_PRICE_MIN_PCT and rsi_delta > _DIV_RSI_MIN:
            rsi_bull_div = True
        # Bearish: price higher (positive move), RSI lower (negative delta)
        elif price_move > _DIV_PRICE_MIN_PCT and rsi_delta < -_DIV_RSI_MIN:
            rsi_bear_div = True

    # ── Stochastic(5, 3, 3) ───────────────────────────────────────────────────
    stoch_k_val, stoch_d_val, stoch_k_prev_val = _stochastic(high, low, close, k=5, smooth_k=3, d=3)

    # ── Momentum(5) ───────────────────────────────────────────────────────────
    if n > 5:
        mom_now  = float(close.iloc[-1]) - float(close.iloc[-6])
        mom_prev = float(close.iloc[-2]) - float(close.iloc[-7]) if n > 6 else 0.0
    else:
        mom_now = mom_prev = 0.0

    # ── ATR(10) ───────────────────────────────────────────────────────────────
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)

    atr_val   = float(tr.rolling(10).mean().iloc[-1]) if n >= 10 else float(tr.mean())
    hist_n    = min(30, n)
    atr_avg   = float(tr.rolling(10).mean().iloc[-hist_n:].mean()) if n >= 10 else atr_val
    atr_r     = atr_val / atr_avg if atr_avg > 0 else 1.0

    # ── Bollinger Bands(15, 2.0) ──────────────────────────────────────────────
    bb_p      = min(15, n)
    bb_mid_s  = close.rolling(bb_p).mean()
    bb_std_s  = close.rolling(bb_p).std()
    bb_u      = float((bb_mid_s + 2.0 * bb_std_s).iloc[-1])
    bb_l      = float((bb_mid_s - 2.0 * bb_std_s).iloc[-1])
    bb_m      = float(bb_mid_s.iloc[-1])
    bb_bw_now = (bb_u - bb_l) / bb_m if bb_m else 0.0

    if n >= bb_p + 5:
        bb_bw_p = float((bb_mid_s + 2.0 * bb_std_s).iloc[-6] - (bb_mid_s - 2.0 * bb_std_s).iloc[-6])
        bb_bw_p = bb_bw_p / float(bb_mid_s.iloc[-6]) if float(bb_mid_s.iloc[-6]) else bb_bw_now
    else:
        bb_bw_p = bb_bw_now

    # ── ADX(14) — Wilder's method ─────────────────────────────────────────────
    adx_val, plus_di, minus_di = _adx(high, low, close, period=14)

    return Indicators(
        ema5=float(ema5_s.iloc[-1]),
        ema13=float(ema13_s.iloc[-1]),
        ema21=float(ema21_s.iloc[-1]),
        ema5_series=ema5_s,
        ema13_series=ema13_s,
        ema21_series=ema21_s,
        rsi=rsi_val,
        rsi_prev=rsi_prev_val,
        rsi_series=rsi_s,
        rsi_bull_div=rsi_bull_div,
        rsi_bear_div=rsi_bear_div,
        stoch_k=stoch_k_val,
        stoch_d=stoch_d_val,
        stoch_k_prev=stoch_k_prev_val,
        momentum=mom_now,
        momentum_prev=mom_prev,
        atr=atr_val,
        atr_avg30=atr_avg,
        atr_ratio=atr_r,
        bb_upper=bb_u,
        bb_lower=bb_l,
        bb_mid=bb_m,
        bb_bw=bb_bw_now,
        bb_bw_prev=bb_bw_p,
        adx=adx_val,
        adx_plus_di=plus_di,
        adx_minus_di=minus_di,
    )


# ── Internal helpers ──────────────────────────────────────────────────────────

def _rsi_series(close: pd.Series, period: int = 7) -> pd.Series:
    """Return a full RSI series (same length as close). Uses pandas_ta if available."""
    try:
        import pandas_ta as ta
        s = ta.rsi(close, length=period)
        if s is not None and len(s) > 0:
            s = s.fillna(50.0)
            return s
    except Exception:
        pass
    # Manual Wilder RSI fallback
    delta = close.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    ag    = gain.ewm(com=period - 1, min_periods=period).mean()
    al    = loss.ewm(com=period - 1, min_periods=period).mean()
    rs    = ag / al.replace(0, np.nan)
    r     = (100 - (100 / (1 + rs))).fillna(50.0)
    return r


def _rsi(close: pd.Series, period: int = 7) -> float:
    """Legacy single-value RSI — kept for backward compat."""
    s = _rsi_series(close, period)
    v = float(s.iloc[-1])
    return v if not np.isnan(v) else 50.0


def _stochastic(
    high: pd.Series, low: pd.Series, close: pd.Series,
    k: int = 5, smooth_k: int = 3, d: int = 3,
) -> tuple[float, float, float]:
    """Returns (smooth_k_now, d_now, smooth_k_prev)."""
    try:
        import pandas_ta as ta
        s = ta.stoch(high, low, close, k=k, d=d, smooth_k=smooth_k)
        col_k = f"STOCHk_{k}_{d}_{smooth_k}"
        col_d = f"STOCHd_{k}_{d}_{smooth_k}"
        if s is not None and col_k in s.columns:
            kv = float(s[col_k].iloc[-1])
            dv = float(s[col_d].iloc[-1]) if col_d in s.columns else kv
            kp = float(s[col_k].iloc[-2]) if len(s) >= 2 else kv
            return (
                kv if not np.isnan(kv) else 50.0,
                dv if not np.isnan(dv) else 50.0,
                kp if not np.isnan(kp) else 50.0,
            )
    except Exception:
        pass
    # Manual fallback
    ll   = low.rolling(k).min()
    hh   = high.rolling(k).max()
    denom = (hh - ll).replace(0, np.nan)
    raw_k = 100 * (close - ll) / denom
    sk    = raw_k.rolling(smooth_k).mean()
    sd    = sk.rolling(d).mean()
    kv    = float(sk.iloc[-1]) if not np.isnan(float(sk.iloc[-1])) else 50.0
    dv    = float(sd.iloc[-1]) if not np.isnan(float(sd.iloc[-1])) else kv
    kp    = float(sk.iloc[-2]) if len(sk) >= 2 else kv
    return kv, dv, kp


def _adx(
    high: pd.Series, low: pd.Series, close: pd.Series,
    period: int = 14,
) -> tuple[float, float, float]:
    """
    Wilder's ADX(14). Returns (adx, +DI, -DI).
    adx: 0-100 — directional movement index (trend strength, not direction)
    +DI > -DI → bullish; -DI > +DI → bearish
    """
    n = len(close)
    if n < period + 2:
        return 20.0, 25.0, 25.0   # neutral defaults when not enough data

    high_s  = high.reset_index(drop=True)
    low_s   = low.reset_index(drop=True)
    close_s = close.reset_index(drop=True)

    # True Range
    tr = pd.concat([
        high_s - low_s,
        (high_s - close_s.shift(1)).abs(),
        (low_s  - close_s.shift(1)).abs(),
    ], axis=1).max(axis=1)

    # Directional Movement
    up_move   = high_s.diff()
    down_move = -(low_s.diff())

    plus_dm  = np.where((up_move > down_move) & (up_move > 0),  up_move,   0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    plus_dm_s  = pd.Series(plus_dm,  index=close.index)
    minus_dm_s = pd.Series(minus_dm, index=close.index)

    # Wilder smoothing (equivalent to EWM with alpha = 1/period)
    alpha = 1.0 / period
    atr_w   = tr.ewm(alpha=alpha, adjust=False).mean()
    plus_di_s  = 100 * plus_dm_s.ewm(alpha=alpha, adjust=False).mean() / atr_w.replace(0, np.nan)
    minus_di_s = 100 * minus_dm_s.ewm(alpha=alpha, adjust=False).mean() / atr_w.replace(0, np.nan)

    plus_di_s  = plus_di_s.fillna(25.0)
    minus_di_s = minus_di_s.fillna(25.0)

    # DX and ADX
    di_sum  = (plus_di_s + minus_di_s).replace(0, np.nan)
    dx      = 100 * (plus_di_s - minus_di_s).abs() / di_sum
    dx      = dx.fillna(0.0)
    adx_s   = dx.ewm(alpha=alpha, adjust=False).mean()

    adx_val    = float(adx_s.iloc[-1])
    plus_di_v  = float(plus_di_s.iloc[-1])
    minus_di_v = float(minus_di_s.iloc[-1])

    # Clamp to valid range
    adx_val    = max(0.0, min(100.0, adx_val))
    plus_di_v  = max(0.0, min(100.0, plus_di_v))
    minus_di_v = max(0.0, min(100.0, minus_di_v))

    return adx_val, plus_di_v, minus_di_v
