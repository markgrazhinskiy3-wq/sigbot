"""
Market Mode Detection — Layer 2
Classifies current market into one of 5 modes that determine which strategies to run.

Modes:
  TRENDING_UP   — clear uptrend on 1-min and 5-min
  TRENDING_DOWN — clear downtrend on 1-min and 5-min
  RANGE         — oscillating between identifiable S/R
  VOLATILE      — ATR spike, large candles, no clear direction
  SQUEEZE       — compressed ATR, Bollinger Bands narrowing — pending breakout
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class MarketMode:
    mode: str           # TRENDING_UP | TRENDING_DOWN | RANGE | VOLATILE | SQUEEZE
    strength: float     # 0-100 — how clearly the mode is established
    trend_up: bool      # True if any upward bias (used for 5-min context)
    trend_down: bool
    explanation: str
    debug: dict


def detect_market_mode(df1m: pd.DataFrame, df5m: pd.DataFrame | None = None) -> MarketMode:
    """
    Determine market mode from 1-min candles (primary) and 5-min candles (context).
    Requires at least 25 1-min candles.
    """
    n = len(df1m)
    if n < 15:
        return MarketMode("RANGE", 40.0, False, False,
                          "Мало данных — считаем боковым рынком", {"n": n})

    close = df1m["close"]
    high  = df1m["high"]
    low   = df1m["low"]
    open_ = df1m["open"]

    # ── EMAs (1-min) ─────────────────────────────────────────────────────────
    ema5  = close.ewm(span=5,  adjust=False).mean()
    ema13 = close.ewm(span=13, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()

    ema5_now  = float(ema5.iloc[-1])
    ema13_now = float(ema13.iloc[-1])
    ema21_now = float(ema21.iloc[-1])

    # EMA alignment check over last 5 candles
    lookback = min(5, n)
    ema5_arr  = ema5.iloc[-lookback:].values
    ema13_arr = ema13.iloc[-lookback:].values
    ema21_arr = ema21.iloc[-lookback:].values

    ema_bull_bars = int(np.sum((ema5_arr > ema13_arr) & (ema13_arr > ema21_arr)))
    ema_bear_bars = int(np.sum((ema5_arr < ema13_arr) & (ema13_arr < ema21_arr)))

    ema5_vs_21_spread = abs(ema5_now - ema21_now) / float(close.iloc[-1]) * 100 if float(close.iloc[-1]) else 0.0
    ema_aligned_up   = ema_bull_bars >= 4
    ema_aligned_down = ema_bear_bars >= 4
    ema_flat         = ema5_vs_21_spread < 0.03

    # ── ATR ───────────────────────────────────────────────────────────────────
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)

    atr10     = float(tr.rolling(10).mean().iloc[-1]) if n >= 10 else float(tr.mean())
    hist_bars = min(30, n)
    atr_hist  = float(tr.rolling(10).mean().iloc[-hist_bars:].mean()) if n >= 10 else atr10
    atr_ratio = atr10 / atr_hist if atr_hist > 0 else 1.0

    # ── Bollinger Bands (15, 2.0) ─────────────────────────────────────────────
    bb_period = min(15, n)
    bb_mid    = close.rolling(bb_period).mean()
    bb_std    = close.rolling(bb_period).std()
    bb_bw_now = float((bb_std * 4 / bb_mid).iloc[-1]) if float(bb_mid.iloc[-1]) else 0.0

    # Bandwidth trend: is it narrowing?
    if n >= bb_period + 5:
        bb_bw_prev = float((bb_std * 4 / bb_mid).iloc[-6])
        bb_narrowing = bb_bw_now < bb_bw_prev * 0.9
    else:
        bb_narrowing = False

    # ── Candle direction stats ────────────────────────────────────────────────
    recent_n   = min(7, n)
    bull_pct   = float((close.iloc[-recent_n:] > open_.iloc[-recent_n:]).mean() * 100)
    body_abs   = (close - open_).abs()
    avg_body   = float(body_abs.rolling(min(10, n)).mean().iloc[-1])
    last5_body = float(body_abs.iloc[-min(5, n):].mean())
    body_ratio = last5_body / avg_body if avg_body > 0 else 1.0

    # ── Swing structure (last 20 bars) ────────────────────────────────────────
    lb   = min(20, n)
    seg  = max(3, lb // 3)
    h    = high.iloc[-lb:]
    l    = low.iloc[-lb:]
    h1   = float(h.iloc[:seg].max()); h2 = float(h.iloc[seg:2*seg].max()); h3 = float(h.iloc[2*seg:].max())
    l1   = float(l.iloc[:seg].min()); l2 = float(l.iloc[seg:2*seg].min()); l3 = float(l.iloc[2*seg:].min())
    hh   = h3 > h2 > h1
    hl   = l3 > l2 > l1
    lh   = h3 < h2 < h1
    ll   = l3 < l2 < l1

    # ── 5-min context ─────────────────────────────────────────────────────────
    ctx_up   = False
    ctx_down = False
    if df5m is not None and len(df5m) >= 5:
        c5   = df5m["close"]
        e21  = float(c5.ewm(span=min(21, len(df5m)), adjust=False).mean().iloc[-1])
        e50  = float(c5.ewm(span=min(50, len(df5m)), adjust=False).mean().iloc[-1])
        ctx_up   = e21 > e50
        ctx_down = e21 < e50

    # ── VOLATILE: ATR spike + large candles + no clear direction ──────────────
    is_volatile = (
        atr_ratio > 1.5
        and body_ratio > 1.3
        and not ema_aligned_up
        and not ema_aligned_down
    )
    if is_volatile:
        return MarketMode(
            "VOLATILE", min(100, atr_ratio * 40),
            ctx_up, ctx_down,
            f"Высокая волатильность: ATR×{atr_ratio:.2f}, тела ×{body_ratio:.2f}",
            {"atr_ratio": round(atr_ratio, 2), "body_ratio": round(body_ratio, 2)}
        )

    # ── SQUEEZE: compressed ATR + narrowing BB + small bodies ─────────────────
    is_squeeze = (
        atr_ratio < 0.6
        and bb_narrowing
        and body_ratio < 0.5
    )
    if is_squeeze:
        return MarketMode(
            "SQUEEZE", min(100, (1 - atr_ratio) * 80),
            ctx_up, ctx_down,
            f"Сжатие: ATR×{atr_ratio:.2f}, BB сужаются, тела ×{body_ratio:.2f}",
            {"atr_ratio": round(atr_ratio, 2), "bb_narrowing": bb_narrowing}
        )

    # ── TRENDING_UP ───────────────────────────────────────────────────────────
    up_pts = 0
    up_pts += 40 if ema_aligned_up else 0
    up_pts += 25 if (hh and hl) else (12 if (hh or hl) else 0)
    up_pts += 20 if bull_pct > 65 else (10 if bull_pct > 55 else 0)
    up_pts += 10 if ctx_up else 0

    # ── TRENDING_DOWN ─────────────────────────────────────────────────────────
    dn_pts = 0
    dn_pts += 40 if ema_aligned_down else 0
    dn_pts += 25 if (lh and ll) else (12 if (lh or ll) else 0)
    dn_pts += 20 if bull_pct < 35 else (10 if bull_pct < 45 else 0)
    dn_pts += 10 if ctx_down else 0

    if up_pts >= 55 and up_pts > dn_pts + 15:
        return MarketMode(
            "TRENDING_UP", min(100.0, float(up_pts)),
            True, False,
            f"Восходящий тренд: EMA выровнены={ema_aligned_up}, свинги HH={hh}/HL={hl}, бычьих {bull_pct:.0f}%",
            {"up_pts": up_pts, "ema_aligned": ema_aligned_up, "bull_pct": round(bull_pct)}
        )

    if dn_pts >= 55 and dn_pts > up_pts + 15:
        return MarketMode(
            "TRENDING_DOWN", min(100.0, float(dn_pts)),
            False, True,
            f"Нисходящий тренд: EMA выровнены={ema_aligned_down}, свинги LH={lh}/LL={ll}, медвежьих {100-bull_pct:.0f}%",
            {"dn_pts": dn_pts, "ema_aligned": ema_aligned_down, "bear_pct": round(100 - bull_pct)}
        )

    # ── RANGE ─────────────────────────────────────────────────────────────────
    range_strength = max(0, 80 - abs(up_pts - dn_pts) * 2) if ema_flat else 60
    return MarketMode(
        "RANGE", float(range_strength),
        ctx_up, ctx_down,
        f"Боковой рынок: EMA flat={ema_flat}, ATR×{atr_ratio:.2f}",
        {"up_pts": up_pts, "dn_pts": dn_pts, "ema_flat": ema_flat}
    )
