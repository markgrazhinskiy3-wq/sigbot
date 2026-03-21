"""
Level Analysis
Finds local support/resistance levels and evaluates distance to each.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass, field


@dataclass
class LevelAnalysisResult:
    supports: list        # list of float price levels (support)
    resistances: list     # list of float price levels (resistance)
    nearest_support: float
    nearest_resistance: float
    distance_to_support_pct: float    # % below current price
    distance_to_resistance_pct: float # % above current price
    buy_score: float      # 0-100 (high = good room above, not stuck under resistance)
    sell_score: float     # 0-100 (high = good room below, not stuck above support)
    explanation: str


def _swing_highs(high: pd.Series, window: int = 3) -> list:
    levels = []
    for i in range(window, len(high) - window):
        if float(high.iloc[i]) == float(high.iloc[i - window: i + window + 1].max()):
            levels.append(float(high.iloc[i]))
    return levels


def _swing_lows(low: pd.Series, window: int = 3) -> list:
    levels = []
    for i in range(window, len(low) - window):
        if float(low.iloc[i]) == float(low.iloc[i - window: i + window + 1].min()):
            levels.append(float(low.iloc[i]))
    return levels


def _cluster(levels: list, tol_pct: float = 0.003) -> list:
    if not levels:
        return []
    levels = sorted(levels)
    clusters = []
    group = [levels[0]]
    for v in levels[1:]:
        if group[0] > 0 and (v - group[-1]) / group[0] <= tol_pct:
            group.append(v)
        else:
            clusters.append(float(np.mean(group)))
            group = [v]
    clusters.append(float(np.mean(group)))
    return clusters


def level_analysis(df: pd.DataFrame) -> LevelAnalysisResult:
    n = len(df)
    price = float(df["close"].iloc[-1])

    if n < 15 or price == 0:
        return LevelAnalysisResult(
            [], [], price * 0.998, price * 1.002,
            0.2, 0.2, 50.0, 50.0, "Недостаточно данных"
        )

    high  = df["high"]
    low   = df["low"]

    raw_res = _swing_highs(high, window=3)
    raw_sup = _swing_lows(low,  window=3)

    resistances = sorted([r for r in _cluster(raw_res) if r > price * 0.998])
    supports    = sorted([s for s in _cluster(raw_sup) if s < price * 1.002], reverse=True)

    # Fallback: use recent range if no levels found
    if not resistances:
        resistances = [float(high.iloc[-20:].max())]
    if not supports:
        supports = [float(low.iloc[-20:].min())]

    nearest_res = resistances[0] if resistances else price * 1.005
    nearest_sup = supports[0]    if supports    else price * 0.995

    dist_res_pct = (nearest_res - price) / price * 100 if nearest_res > price else 0.0
    dist_sup_pct = (price - nearest_sup) / price * 100 if nearest_sup < price else 0.0

    # ── Score: penalise when price is very close to opposing level ────────────
    # BUY: penalise if stuck just under resistance (small headroom)
    # SELL: penalise if stuck just above support (small room below)

    CLOSE_THRESHOLD = 0.15   # 0.15% = "too close"
    GOOD_THRESHOLD  = 0.40   # 0.40% = healthy room

    if dist_res_pct <= CLOSE_THRESHOLD:
        buy_score = 15.0     # wall just above — terrible for BUY
    elif dist_res_pct <= GOOD_THRESHOLD:
        buy_score = 40.0     # close but passable
    else:
        buy_score = 80.0     # good headroom for BUY

    if dist_sup_pct <= CLOSE_THRESHOLD:
        sell_score = 15.0    # floor just below — terrible for SELL
    elif dist_sup_pct <= GOOD_THRESHOLD:
        sell_score = 40.0
    else:
        sell_score = 80.0

    parts = []
    if dist_res_pct <= CLOSE_THRESHOLD:
        parts.append(f"Сопротивление в {dist_res_pct:.2f}% — нет места для BUY")
    else:
        parts.append(f"До сопротивления {dist_res_pct:.2f}%")
    if dist_sup_pct <= CLOSE_THRESHOLD:
        parts.append(f"Поддержка в {dist_sup_pct:.2f}% — нет места для SELL")
    else:
        parts.append(f"До поддержки {dist_sup_pct:.2f}%")

    return LevelAnalysisResult(
        supports=supports,
        resistances=resistances,
        nearest_support=nearest_sup,
        nearest_resistance=nearest_res,
        distance_to_support_pct=dist_sup_pct,
        distance_to_resistance_pct=dist_res_pct,
        buy_score=buy_score,
        sell_score=sell_score,
        explanation="; ".join(parts),
    )
