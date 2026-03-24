"""
Pattern: Compression Breakout
Market compresses into a tight range, then breaks out with momentum.

Conditions:
  - Last 6-10 bars have shrinking range (ATR compression)
  - Breakout candle body significantly larger than compressed avg
  - Breakout close is outside the compressed range boundary
  - No immediate full retrace

BUY  = breakout up
SELL = breakout down

fit_for: ["1m", "2m"] — clean momentum; both expiries work
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class PatternResult:
    name: str
    direction: str
    score: float
    fit_for: list[str]
    pattern_quality: float
    level_quality: float
    rejection_reason: str
    debug: dict


_COMPRESSION_BARS  = 8     # look at last N bars for range measurement
_EXPANSION_RATIO   = 1.6   # breakout body must be >= X * compressed avg body
_RANGE_RATIO       = 0.80  # recent ATR must be <= X * baseline ATR to call "compressed" (was 0.75)
_MIN_ATR_RATIO     = 0.30  # don't trade if market is dead (ATR vs baseline)
_MIN_SCORE         = 38.0


def detect_compression_breakout(df: pd.DataFrame) -> PatternResult:
    n = len(df)
    if n < 15:
        return _none("n<15")

    cl  = df["close"].values
    op  = df["open"].values
    hi  = df["high"].values
    lo  = df["low"].values

    price     = float(cl[-1])
    body_abs  = np.abs(cl - op)
    ranges    = hi - lo

    # ── Compression window: last 6-10 bars BEFORE the current bar ────────────
    comp_n     = min(_COMPRESSION_BARS, n - 2)
    comp_end   = n - 1   # exclude current (breakout) bar
    comp_start = comp_end - comp_n

    comp_bodies = body_abs[comp_start:comp_end]
    comp_ranges = ranges[comp_start:comp_end]
    comp_highs  = hi[comp_start:comp_end]
    comp_lows   = lo[comp_start:comp_end]

    comp_body_avg = float(np.mean(comp_bodies)) or 1e-8
    comp_rng_avg  = float(np.mean(comp_ranges)) or 1e-8

    # ── Baseline: bars BEFORE the compression window (not overlapping it) ─────
    # This makes the comparison honest — compressed bars don't pollute baseline
    pre_window = comp_start   # all bars before compression
    if pre_window >= 8:
        baseline_body = float(np.mean(body_abs[:pre_window])) or 1e-8
        baseline_rng  = float(np.mean(ranges[:pre_window])) or 1e-8
    elif pre_window >= 4:
        baseline_body = float(np.mean(body_abs[:pre_window])) or 1e-8
        baseline_rng  = float(np.mean(ranges[:pre_window])) or 1e-8
    else:
        # Fallback: use last 20 bars but compare with strict ratio
        baseline_body = float(np.mean(body_abs[-min(20, n):])) or 1e-8
        baseline_rng  = float(np.mean(ranges[-min(20, n):])) or 1e-8

    # ── Compression check ─────────────────────────────────────────────────────
    is_compressed = (
        comp_body_avg < baseline_body * _RANGE_RATIO and
        comp_rng_avg  < baseline_rng  * _RANGE_RATIO
    )
    if not is_compressed:
        return _none(
            f"not_compressed: comp_body={comp_body_avg:.6f} ({comp_body_avg/baseline_body:.2f}x base)"
        )

    # Compression quality: how tight relative to baseline
    compression_q = max(0.0, 1.0 - comp_body_avg / baseline_body) * 100

    # ── Breakout candle (current bar) ─────────────────────────────────────────
    curr_body    = float(body_abs[-1])
    curr_close   = float(cl[-1])
    curr_open    = float(op[-1])
    curr_high    = float(hi[-1])
    curr_low     = float(lo[-1])

    range_high = float(np.max(comp_highs))
    range_low  = float(np.min(comp_lows))

    # Breakout direction
    broke_up   = curr_close > range_high and curr_close > curr_open
    broke_down = curr_close < range_low  and curr_close < curr_open

    if not broke_up and not broke_down:
        return _none(
            f"no_breakout: close={curr_close:.6f} range=[{range_low:.6f},{range_high:.6f}]"
        )

    is_buy = broke_up

    # ── Breakout strength ─────────────────────────────────────────────────────
    expansion_ratio = curr_body / comp_body_avg
    if expansion_ratio < _EXPANSION_RATIO:
        return _none(
            f"weak_body: curr={curr_body:.6f} comp_avg={comp_body_avg:.6f} ratio={expansion_ratio:.2f}"
        )

    # How far outside the range did the close go?
    if is_buy:
        breakout_distance = (curr_close - range_high) / range_high * 100
        shadow = curr_high - curr_close          # upper shadow on buy
        tail   = min(curr_close, curr_open) - curr_low  # lower tail (good for buy)
    else:
        breakout_distance = (range_low - curr_close) / range_low * 100
        shadow = curr_low  - min(curr_close, curr_open)  # lower shadow on sell (but reversed)
        shadow = curr_close - curr_low           # lower shadow = tail on sell is bad
        shadow = max(curr_close, curr_open) - curr_high  # upper shadow (bad for sell)
        shadow = curr_high - max(curr_close, curr_open)

    # Shadow penalty: large shadow against breakout direction weakens signal
    shadow_ratio = shadow / (curr_body + 1e-10)

    # ── Pattern quality ────────────────────────────────────────────────────────
    expansion_q   = min(100.0, (expansion_ratio - _EXPANSION_RATIO) / 2.0 * 60.0 + 40.0)
    distance_q    = min(100.0, breakout_distance / 0.05 * 30.0)
    shadow_pen    = min(40.0, shadow_ratio * 25.0)
    pattern_q     = max(0.0, (compression_q * 0.35 + expansion_q * 0.45 + distance_q * 0.20) - shadow_pen)

    # ── Level quality: not applicable here — use ATR ratio as proxy ───────────
    atr_ratio      = comp_rng_avg / baseline_rng
    level_q        = max(0.0, (1.0 - atr_ratio) * 100.0)   # tighter = better

    # ── Score ──────────────────────────────────────────────────────────────────
    score = pattern_q * 0.65 + level_q * 0.35
    score = min(100.0, score)

    if score < _MIN_SCORE:
        return _none(f"score_too_low={score:.1f}")

    return PatternResult(
        name="compression_breakout",
        direction="BUY" if is_buy else "SELL",
        score=round(score, 1),
        fit_for=["1m", "2m"],
        pattern_quality=round(pattern_q, 1),
        level_quality=round(level_q, 1),
        rejection_reason="",
        debug={
            "compression_q":   round(compression_q, 1),
            "expansion_ratio": round(expansion_ratio, 2),
            "expansion_q":     round(expansion_q, 1),
            "distance_q":      round(distance_q, 1),
            "shadow_pen":      round(shadow_pen, 1),
            "shadow_ratio":    round(shadow_ratio, 2),
            "atr_ratio":       round(atr_ratio, 3),
            "range_high":      round(range_high, 6),
            "range_low":       round(range_low, 6),
            "comp_body_avg":   round(comp_body_avg, 7),
            "baseline_body":   round(baseline_body, 7),
            "breakout_dist":   round(breakout_distance, 4),
        },
    )


def _none(reason: str) -> PatternResult:
    return PatternResult(
        name="compression_breakout",
        direction="NONE",
        score=0.0,
        fit_for=["1m", "2m"],
        pattern_quality=0.0,
        level_quality=0.0,
        rejection_reason=reason,
        debug={"reject": reason},
    )
