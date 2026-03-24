"""
Level Detection V2
Enhanced support/resistance detection with metadata:
  - touches, age, last_touch_idx, reaction_strength
  - is_fresh (touched within last 20 bars)
  - is_broken (price crossed and stayed through level)
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class Level:
    price: float
    type: str             # "support" | "resistance"
    touches: int
    age: int              # bars since first recorded touch
    last_touch_idx: int   # bars from end (0 = current bar)
    reaction_strength: float  # avg move away after touch (as % of price)
    is_fresh: bool        # last touch within recent 20 bars
    is_broken: bool       # price has moved strongly through level from other side


_TOUCH_TOL_PCT  = 0.0012   # 0.12% — price within this = "touching" the level
_CLUSTER_TOL_PCT = 0.0015  # 0.15% — merge nearby pivots into one cluster
_FRESH_BARS     = 20       # last N bars for "is_fresh"
_BROKEN_MOVE    = 0.0010   # 0.10% — move through level that constitutes "broken"


def detect_levels_v2(
    df: pd.DataFrame,
    df1m: pd.DataFrame | None = None,
) -> tuple[list[Level], list[Level]]:
    """
    Detect support and resistance levels with full metadata.

    Args:
        df:   15s OHLC DataFrame (primary — used for recent touch detection)
        df1m: 1m OHLC DataFrame (optional — adds structural swing points)

    Returns:
        (supports, resistances) — both sorted by quality (touches desc, freshness)
    """
    n     = len(df)
    price = float(df["close"].iloc[-1])
    if n < 10 or price == 0:
        return [], []

    # ── Collect raw pivot prices ─────────────────────────────────────────────
    raw_sup_prices = _swing_lows(df["low"],  window=2)
    raw_res_prices = _swing_highs(df["high"], window=2)

    if df1m is not None and len(df1m) >= 10:
        raw_sup_prices += _swing_lows(df1m["low"],  window=2)
        raw_res_prices += _swing_highs(df1m["high"], window=2)

    # ── Cluster pivots ───────────────────────────────────────────────────────
    sup_clusters = _cluster(raw_sup_prices, _CLUSTER_TOL_PCT)
    res_clusters = _cluster(raw_res_prices, _CLUSTER_TOL_PCT)

    # ── Build Level objects ──────────────────────────────────────────────────
    hi  = df["high"].values
    lo  = df["low"].values
    cl  = df["close"].values
    op  = df["open"].values

    supports    = []
    resistances = []

    for lvl_price in sup_clusters:
        if lvl_price >= price * 1.0005:
            continue   # not a support (price is below it)
        meta = _level_meta(lvl_price, hi, lo, cl, op, n, "support", price)
        supports.append(meta)

    for lvl_price in res_clusters:
        if lvl_price <= price * 0.9995:
            continue   # not a resistance (price is above it)
        meta = _level_meta(lvl_price, hi, lo, cl, op, n, "resistance", price)
        resistances.append(meta)

    # Sort: more touches first, then by freshness
    supports.sort(key=lambda l: (l.touches, l.is_fresh, -l.last_touch_idx), reverse=True)
    resistances.sort(key=lambda l: (l.touches, l.is_fresh, -l.last_touch_idx), reverse=True)

    return supports, resistances


def _level_meta(
    lvl_price: float,
    hi: np.ndarray,
    lo: np.ndarray,
    cl: np.ndarray,
    op: np.ndarray,
    n: int,
    level_type: str,
    current_price: float,
) -> Level:
    """Build Level metadata by scanning all bars."""
    tol = lvl_price * _TOUCH_TOL_PCT

    first_touch_idx = n  # bars from end of first touch
    last_touch_idx  = n
    touches         = 0
    reactions: list[float] = []

    for i in range(n - 1, -1, -1):
        bar_from_end = n - 1 - i   # 0 = most recent

        if level_type == "support":
            hit = lo[i] <= lvl_price + tol and lo[i] >= lvl_price - tol * 3
        else:
            hit = hi[i] >= lvl_price - tol and hi[i] <= lvl_price + tol * 3

        if hit:
            touches += 1
            if bar_from_end < last_touch_idx:
                last_touch_idx = bar_from_end
            if bar_from_end > first_touch_idx or first_touch_idx == n:
                first_touch_idx = bar_from_end

            # Reaction: how much did price move away after this touch?
            if i < n - 1:
                if level_type == "support":
                    move = float(cl[i + 1] - lo[i]) / lvl_price * 100
                else:
                    move = float(hi[i] - cl[i + 1]) / lvl_price * 100
                reactions.append(max(0.0, move))

    age = first_touch_idx if first_touch_idx < n else n
    is_fresh = last_touch_idx <= _FRESH_BARS
    reaction_strength = float(np.mean(reactions)) if reactions else 0.0

    # Is broken: price moved strongly through level from other side
    is_broken = False
    if touches >= 1 and last_touch_idx > 2:
        recent_window = min(last_touch_idx, 5)
        for j in range(1, recent_window + 1):
            idx = n - j
            if idx < 0:
                break
            if level_type == "support":
                if cl[idx] < lvl_price - lvl_price * _BROKEN_MOVE:
                    is_broken = True
                    break
            else:
                if cl[idx] > lvl_price + lvl_price * _BROKEN_MOVE:
                    is_broken = True
                    break

    return Level(
        price=round(lvl_price, 8),
        type=level_type,
        touches=touches,
        age=age,
        last_touch_idx=last_touch_idx,
        reaction_strength=round(reaction_strength, 5),
        is_fresh=is_fresh,
        is_broken=is_broken,
    )


def _swing_highs(series: pd.Series, window: int = 2) -> list[float]:
    arr = series.values
    out = []
    for i in range(window, len(arr) - window):
        seg = arr[i - window: i + window + 1]
        if float(arr[i]) >= float(seg.max()):
            out.append(float(arr[i]))
    return out


def _swing_lows(series: pd.Series, window: int = 2) -> list[float]:
    arr = series.values
    out = []
    for i in range(window, len(arr) - window):
        seg = arr[i - window: i + window + 1]
        if float(arr[i]) <= float(seg.min()):
            out.append(float(arr[i]))
    return out


def _cluster(prices: list[float], tol_pct: float) -> list[float]:
    if not prices:
        return []
    prices = sorted(prices)
    groups: list[list[float]] = [[prices[0]]]
    for p in prices[1:]:
        ref = groups[-1][0]
        if ref > 0 and (p - groups[-1][-1]) / ref <= tol_pct:
            groups[-1].append(p)
        else:
            groups.append([p])
    return [float(np.mean(g)) for g in groups]
