"""
Support / Resistance Level Detection
- Detects swing highs/lows with window of 3 bars
- Clusters levels within 0.15% tolerance
- Requires 2+ touches to consider a level significant
- Also merges levels found on 5-min data (stronger levels)
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass, field


@dataclass
class LevelSet:
    supports:     list[float]
    resistances:  list[float]
    strong_sup:   list[float]   # 3+ touches (high-confidence)
    strong_res:   list[float]
    nearest_sup:  float
    nearest_res:  float
    dist_to_sup_pct:  float
    dist_to_res_pct:  float


def detect_levels(df1m: pd.DataFrame, df5m: pd.DataFrame | None = None) -> LevelSet:
    price = float(df1m["close"].iloc[-1])
    if price == 0:
        price = 1.0

    # 1-min swing levels
    raw_res_1m = _swing_highs(df1m["high"], window=3)
    raw_sup_1m = _swing_lows(df1m["low"],  window=3)

    # 5-min swing levels (weighted more — but kept separate so we can merge)
    raw_res_5m: list[float] = []
    raw_sup_5m: list[float] = []
    if df5m is not None and len(df5m) >= 7:
        raw_res_5m = _swing_highs(df5m["high"], window=2)
        raw_sup_5m = _swing_lows(df5m["low"],  window=2)

    # Merge and cluster
    all_res = raw_res_1m + raw_res_5m
    all_sup = raw_sup_1m + raw_sup_5m

    res_clusters = _cluster_with_touches(all_res, tol_pct=0.0015)
    sup_clusters = _cluster_with_touches(all_sup, tol_pct=0.0015)

    # Filter relevant levels
    resistances = sorted(
        [l for l, t in res_clusters if l > price * 0.9998],
        key=lambda x: x
    )
    supports = sorted(
        [l for l, t in sup_clusters if l < price * 1.0002],
        key=lambda x: x,
        reverse=True
    )

    strong_res = sorted(
        [l for l, t in res_clusters if l > price * 0.9998 and t >= 2],
        key=lambda x: x
    )
    strong_sup = sorted(
        [l for l, t in sup_clusters if l < price * 1.0002 and t >= 2],
        key=lambda x: x,
        reverse=True
    )

    # Fallback if nothing found
    if not resistances:
        resistances = [float(df1m["high"].iloc[-20:].max())]
    if not supports:
        supports = [float(df1m["low"].iloc[-20:].min())]

    nearest_res = resistances[0] if resistances else price * 1.005
    nearest_sup = supports[0]    if supports    else price * 0.995

    dist_res = max(0.0, (nearest_res - price) / price * 100)
    dist_sup = max(0.0, (price - nearest_sup) / price * 100)

    return LevelSet(
        supports=supports,
        resistances=resistances,
        strong_sup=strong_sup,
        strong_res=strong_res,
        nearest_sup=nearest_sup,
        nearest_res=nearest_res,
        dist_to_sup_pct=round(dist_sup, 4),
        dist_to_res_pct=round(dist_res, 4),
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _swing_highs(series: pd.Series, window: int = 3) -> list[float]:
    result = []
    arr = series.values
    for i in range(window, len(arr) - window):
        if arr[i] == arr[max(0, i - window): i + window + 1].max():
            result.append(float(arr[i]))
    return result


def _swing_lows(series: pd.Series, window: int = 3) -> list[float]:
    result = []
    arr = series.values
    for i in range(window, len(arr) - window):
        if arr[i] == arr[max(0, i - window): i + window + 1].min():
            result.append(float(arr[i]))
    return result


def _cluster_with_touches(levels: list[float], tol_pct: float = 0.0015) -> list[tuple[float, int]]:
    """
    Cluster nearby levels and count touches (occurrences in same cluster).
    Returns list of (level_price, touch_count).
    """
    if not levels:
        return []
    levels = sorted(levels)
    clusters: list[tuple[float, int]] = []
    group = [levels[0]]

    for v in levels[1:]:
        if group[0] > 0 and (v - group[-1]) / group[0] <= tol_pct:
            group.append(v)
        else:
            clusters.append((float(np.mean(group)), len(group)))
            group = [v]
    clusters.append((float(np.mean(group)), len(group)))
    return clusters
