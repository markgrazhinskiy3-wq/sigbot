"""
Indicator Calculator — fast periods tuned for 1-minute binary options.

Periods:
  EMA(5), EMA(13), EMA(21)
  RSI(7)
  Stochastic(5, 3, 3)
  Momentum(5)
  ATR(10)
  Bollinger Bands(15, 2.0)
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass


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

    # ── RSI(7) ────────────────────────────────────────────────────────────────
    rsi_val      = _rsi(close, 7)
    rsi_prev_val = _rsi(close.iloc[:-1], 7) if n >= 9 else rsi_val

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

    return Indicators(
        ema5=float(ema5_s.iloc[-1]),
        ema13=float(ema13_s.iloc[-1]),
        ema21=float(ema21_s.iloc[-1]),
        ema5_series=ema5_s,
        ema13_series=ema13_s,
        ema21_series=ema21_s,
        rsi=rsi_val,
        rsi_prev=rsi_prev_val,
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
    )


# ── Internal helpers ──────────────────────────────────────────────────────────

def _rsi(close: pd.Series, period: int = 7) -> float:
    try:
        import pandas_ta as ta
        s = ta.rsi(close, length=period)
        v = float(s.iloc[-1]) if s is not None else 50.0
        return v if not np.isnan(v) else 50.0
    except Exception:
        delta = close.diff()
        gain  = delta.clip(lower=0)
        loss  = (-delta).clip(lower=0)
        ag    = gain.ewm(com=period - 1, min_periods=period).mean()
        al    = loss.ewm(com=period - 1, min_periods=period).mean()
        rs    = ag / al.replace(0, np.nan)
        r     = 100 - (100 / (1 + rs))
        v     = float(r.iloc[-1])
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
