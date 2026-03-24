"""
Decision Engine V3 — Level MACD Architecture

Uses 1-minute candles exclusively (df1m_ctx).
Two strategies: Level Bounce + Level Break.
Indicators: RSI(14), MACD(12,26,9), CCI(20), ATR(14), EMA(20).
No external TA libraries — pure numpy/pandas.

Signal priority:
  bounce + break (same direction) → 5 stars (100 pts)
  bounce only                     → 4 stars (80 pts)
  break only                      → 3 stars (60 pts)
"""
from __future__ import annotations
import numpy as np
import pandas as pd

from .decision_engine import EngineResult

_MIN_BARS          = 20       # minimum 1m candles needed
_LEVEL_TOUCH_PCT   = 0.0005  # 0.05% — touch zone for clustering
_LEVEL_MIN_TOUCHES = 2       # minimum touches to form a level
_LEVEL_STRONG      = 3       # touches required for Level Break
_BOUNCE_ZONE_PCT   = 0.0008  # 0.08% — price must be within this of level
_DEAD_ATR_PCT      = 0.00001 # ATR < 0.001% of price = frozen market


# ── Indicator helpers ──────────────────────────────────────────────────────────

def _rsi(close: np.ndarray, period: int = 14) -> float:
    if len(close) < period + 1:
        return 50.0
    deltas = np.diff(close[-(period + 1):])
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_g  = float(np.mean(gains))
    avg_l  = float(np.mean(losses))
    if avg_l == 0:
        return 100.0
    return float(100.0 - 100.0 / (1.0 + avg_g / avg_l))


def _ema_arr(values: np.ndarray, period: int) -> np.ndarray:
    out = np.full(len(values), np.nan)
    if len(values) < period:
        return out
    alpha = 2.0 / (period + 1)
    out[period - 1] = float(np.mean(values[:period]))
    for i in range(period, len(values)):
        out[i] = alpha * values[i] + (1.0 - alpha) * out[i - 1]
    return out


def _macd_hist(close: np.ndarray) -> np.ndarray:
    """Returns MACD histogram array (MACD line − signal line)."""
    if len(close) < 35:
        return np.zeros(len(close))
    ema12      = _ema_arr(close, 12)
    ema26      = _ema_arr(close, 26)
    macd_line  = ema12 - ema26
    valid      = ~np.isnan(macd_line)
    if valid.sum() < 9:
        return np.zeros(len(close))
    valid_idx  = np.where(valid)[0]
    sig_vals   = _ema_arr(macd_line[valid], 9)
    signal     = np.full(len(close), np.nan)
    signal[valid_idx] = sig_vals
    hist       = macd_line - signal
    hist[np.isnan(hist)] = 0.0
    return hist


def _cci(hi: np.ndarray, lo: np.ndarray, cl: np.ndarray, period: int = 20) -> float:
    if len(cl) < period:
        return 0.0
    tp      = (hi[-period:] + lo[-period:] + cl[-period:]) / 3.0
    tp_mean = float(np.mean(tp))
    mad     = float(np.mean(np.abs(tp - tp_mean)))
    if mad == 0:
        return 0.0
    return float((float(tp[-1]) - tp_mean) / (0.015 * mad))


def _atr(hi: np.ndarray, lo: np.ndarray, cl: np.ndarray, period: int = 14) -> float:
    if len(cl) < period + 1:
        return float(np.mean(hi[-period:] - lo[-period:]))
    tr = np.maximum(
        hi[1:] - lo[1:],
        np.maximum(np.abs(hi[1:] - cl[:-1]), np.abs(lo[1:] - cl[:-1])),
    )
    return float(np.mean(tr[-period:]))


# ── Level detection ────────────────────────────────────────────────────────────

def _detect_levels(
    hi: np.ndarray, lo: np.ndarray, price: float,
) -> tuple[list[dict], list[dict]]:
    """
    Cluster all bar highs and lows into support/resistance levels.
    Level = zone where 2+ touches occurred within 0.05% of each other.
    """
    tol    = price * _LEVEL_TOUCH_PCT
    points = [(float(v), "res") for v in hi] + [(float(v), "sup") for v in lo]
    used   = [False] * len(points)
    levels: list[dict] = []

    for i, (p1, kind) in enumerate(points):
        if used[i]:
            continue
        cluster = [p1]
        for j in range(i + 1, len(points)):
            if not used[j] and abs(points[j][0] - p1) <= tol:
                cluster.append(points[j][0])
                used[j] = True
        used[i] = True
        if len(cluster) < _LEVEL_MIN_TOUCHES:
            continue
        levels.append({
            "price":   float(np.mean(cluster)),
            "touches": len(cluster),
            "kind":    kind,
        })

    supports    = sorted(
        [lv for lv in levels if lv["price"] < price],
        key=lambda x: x["price"], reverse=True,  # nearest support first
    )
    resistances = sorted(
        [lv for lv in levels if lv["price"] >= price],
        key=lambda x: x["price"],                # nearest resistance first
    )
    return supports, resistances


# ── Strategy 1: Level Bounce ───────────────────────────────────────────────────

def _level_bounce(
    price: float,
    rsi: float, cci: float, hist: np.ndarray,
    supports: list[dict], resistances: list[dict],
    body: float, upper_shadow: float, lower_shadow: float,
) -> tuple[str, dict | None]:
    if len(hist) < 2:
        return "NONE", None
    macd_now  = float(hist[-1])
    macd_prev = float(hist[-2])

    # BUY — bounce from support
    for lvl in supports[:4]:
        if abs(price - lvl["price"]) / price > _BOUNCE_ZONE_PCT:
            continue
        if macd_now <= macd_prev:     # histogram must turn up
            continue
        if lower_shadow <= body:      # wick must extend below body
            continue
        if rsi > 70:                  # not overbought
            continue
        if cci >= 50:                 # room to move up
            continue
        return "BUY", {"level": lvl["price"], "touches": lvl["touches"],
                        "dist_pct": round(abs(price - lvl["price"]) / price * 100, 4)}

    # SELL — bounce from resistance
    for lvl in resistances[:4]:
        if abs(price - lvl["price"]) / price > _BOUNCE_ZONE_PCT:
            continue
        if macd_now >= macd_prev:     # histogram must turn down
            continue
        if upper_shadow <= body:      # wick must extend above body
            continue
        if rsi < 30:                  # not oversold
            continue
        if cci <= -50:                # room to move down
            continue
        return "SELL", {"level": lvl["price"], "touches": lvl["touches"],
                         "dist_pct": round(abs(price - lvl["price"]) / price * 100, 4)}

    return "NONE", None


# ── Strategy 2: Level Break ────────────────────────────────────────────────────

def _level_break(
    price: float, open_: float,
    rsi: float, cci: float, hist: np.ndarray,
    supports: list[dict], resistances: list[dict],
    body: float, avg_body: float,
) -> tuple[str, dict | None]:
    if len(hist) < 3:
        return "NONE", None

    # BUY — break above resistance
    for lvl in resistances[:5]:
        if lvl["touches"] < _LEVEL_STRONG:
            continue
        lp = lvl["price"]
        if price <= lp:               # must have closed above
            continue
        if open_ >= lp:               # must have opened below (true break)
            continue
        if body < avg_body:           # strong impulse candle
            continue
        if not (hist[-1] > hist[-2] > hist[-3]):   # MACD growing 2 bars
            continue
        if not (45 <= rsi <= 75):     # has momentum, not exhausted
            continue
        if cci <= 0:                  # confirms bullish
            continue
        return "BUY", {"level": lp, "touches": lvl["touches"]}

    # SELL — break below support
    for lvl in supports[:5]:
        if lvl["touches"] < _LEVEL_STRONG:
            continue
        lp = lvl["price"]
        if price >= lp:               # must have closed below
            continue
        if open_ <= lp:               # must have opened above
            continue
        if body < avg_body:
            continue
        if not (hist[-1] < hist[-2] < hist[-3]):   # MACD declining 2 bars
            continue
        if not (25 <= rsi <= 55):
            continue
        if cci >= 0:
            continue
        return "SELL", {"level": lp, "touches": lvl["touches"]}

    return "NONE", None


# ── Main engine ────────────────────────────────────────────────────────────────

def run_decision_engine_v3(
    df1m:              pd.DataFrame,
    df5m:              pd.DataFrame | None = None,
    df1m_ctx:          pd.DataFrame | None = None,
    raised_threshold:  bool = False,
    n_bars_15s:        int  = 0,
    n_bars_1m:         int  = 0,
    n_bars_5m:         int  = 0,
    expiry:            str  = "1m",
) -> EngineResult:
    """
    Level MACD engine v3.
    Primary data: df1m_ctx (1-minute candles).
    Falls back to df1m (15s) only if 1m candles unavailable.
    """
    df = df1m_ctx if (df1m_ctx is not None and len(df1m_ctx) >= _MIN_BARS) else df1m
    n  = len(df)

    def _no_sig(reason: str, extra: dict | None = None) -> EngineResult:
        return EngineResult(
            direction="NO_SIGNAL",
            confidence_raw=0.0, stars=0, quality="none",
            strategy_name="", market_mode="RANGE",
            market_mode_strength=50.0, reasoning=reason,
            conditions_met=0, total_conditions=5,
            expiry_hint=expiry,
            debug={"engine": "v3_level_macd", "reason": reason, **(extra or {})},
        )

    if n < _MIN_BARS:
        return _no_sig(f"too_few_candles: n={n} (need {_MIN_BARS} 1m bars)")

    cl    = df["close"].values.astype(float)
    op    = df["open"].values.astype(float)
    hi    = df["high"].values.astype(float)
    lo    = df["low"].values.astype(float)
    price = cl[-1]

    # ── Indicators ────────────────────────────────────────────────────────────
    rsi_val = _rsi(cl, 14)
    hist    = _macd_hist(cl)
    cci_val = _cci(hi, lo, cl, 20)
    atr_val = _atr(hi, lo, cl, 14)
    ema20_v = _ema_arr(cl, 20)
    ema20   = float(ema20_v[-1]) if not np.isnan(ema20_v[-1]) else price

    # ── Dead market filter ────────────────────────────────────────────────────
    if price > 0 and atr_val / price < _DEAD_ATR_PCT:
        return _no_sig(f"dead_market: ATR/price={atr_val/price*100:.5f}% < {_DEAD_ATR_PCT*100:.4f}%")

    # ── Candle anatomy (last bar) ─────────────────────────────────────────────
    body         = abs(cl[-1] - op[-1])
    upper_shadow = hi[-1] - max(cl[-1], op[-1])
    lower_shadow = min(cl[-1], op[-1]) - lo[-1]
    avg_body     = float(np.mean(np.abs(cl[-10:] - op[-10:]))) or 1e-8

    # ── Level detection ───────────────────────────────────────────────────────
    supports, resistances = _detect_levels(hi, lo, price)
    nearest_sup = supports[0]["price"]    if supports    else 0.0
    nearest_res = resistances[0]["price"] if resistances else 0.0

    # ── Run strategies ────────────────────────────────────────────────────────
    bounce_dir, bounce_lvl = _level_bounce(
        price, rsi_val, cci_val, hist,
        supports, resistances, body, upper_shadow, lower_shadow,
    )
    break_dir, break_lvl = _level_break(
        price, op[-1], rsi_val, cci_val, hist,
        supports, resistances, body, avg_body,
    )

    # ── Filter: conflicting directions → no signal ────────────────────────────
    if bounce_dir != "NONE" and break_dir != "NONE" and bounce_dir != break_dir:
        return _no_sig(
            f"conflict: bounce={bounce_dir} break={break_dir}",
            {"bounce_level": bounce_lvl, "break_level": break_lvl},
        )

    # ── Priority and stars ────────────────────────────────────────────────────
    if bounce_dir != "NONE" and break_dir != "NONE":
        direction   = bounce_dir
        stars       = 5
        strategy    = "bounce+break"
        active_lvl  = bounce_lvl
    elif bounce_dir != "NONE":
        direction   = bounce_dir
        stars       = 4
        strategy    = "level_bounce"
        active_lvl  = bounce_lvl
    elif break_dir != "NONE":
        direction   = break_dir
        stars       = 3
        strategy    = "level_break"
        active_lvl  = break_lvl
    else:
        return _no_sig(
            "no_pattern_found",
            {
                "n_supports": len(supports), "n_resistances": len(resistances),
                "rsi": round(rsi_val, 1), "cci": round(cci_val, 1),
                "macd_hist": round(float(hist[-1]), 6) if len(hist) else 0,
            },
        )

    final_score = float(stars * 20)
    quality     = "strong" if stars >= 5 else "moderate" if stars >= 4 else "weak"
    lvl_str     = f"Level: {active_lvl['price']:.5f} ({active_lvl['touches']}x)" if active_lvl else ""
    reason      = f"Strategy: {strategy} | Stars: {stars}" + (f" | {lvl_str}" if lvl_str else "")

    debug = {
        "engine":             "v3_level_macd",
        "strategy":           strategy,
        "rsi":                round(rsi_val, 1),
        "macd_hist":          round(float(hist[-1]), 6) if len(hist) else 0,
        "macd_hist_prev":     round(float(hist[-2]), 6) if len(hist) > 1 else 0,
        "cci":                round(cci_val, 1),
        "atr":                round(atr_val, 6),
        "ema20":              round(ema20, 6),
        "nearest_support":    round(nearest_sup, 6),
        "nearest_resistance": round(nearest_res, 6),
        "level_touches":      active_lvl["touches"] if active_lvl else 0,
        "body":               round(body, 6),
        "upper_shadow":       round(upper_shadow, 6),
        "lower_shadow":       round(lower_shadow, 6),
        "avg_body":           round(avg_body, 6),
        "n_supports":         len(supports),
        "n_resistances":      len(resistances),
        "n_1m_bars":          n,
        "reason":             reason,
        "bounce": {"dir": bounce_dir, "level": bounce_lvl},
        "break":  {"dir": break_dir,  "level": break_lvl},
    }

    return EngineResult(
        direction=direction,
        confidence_raw=final_score,
        stars=stars,
        quality=quality,
        strategy_name=strategy,
        market_mode="TRENDING_UP" if direction == "BUY" else "TRENDING_DOWN",
        market_mode_strength=final_score,
        reasoning=reason,
        conditions_met=stars,
        total_conditions=5,
        expiry_hint=expiry,
        debug=debug,
    )
