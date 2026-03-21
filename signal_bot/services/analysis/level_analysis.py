"""
Level Analysis
Finds local support/resistance and evaluates distance.

Proximity to levels now PENALISES confidence, not blocks outright.
Only extreme proximity (< 0.08%) causes a hard score of 0.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class LevelAnalysisResult:
    supports: list
    resistances: list
    nearest_support: float
    nearest_resistance: float
    distance_to_support_pct: float
    distance_to_resistance_pct: float
    buy_score: float      # 0-100
    sell_score: float     # 0-100
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
    clusters, group = [], [levels[0]]
    for v in levels[1:]:
        if group[0] > 0 and (v - group[-1]) / group[0] <= tol_pct:
            group.append(v)
        else:
            clusters.append(float(np.mean(group)))
            group = [v]
    clusters.append(float(np.mean(group)))
    return clusters


def level_analysis(df: pd.DataFrame) -> LevelAnalysisResult:
    n     = len(df)
    price = float(df["close"].iloc[-1])

    if n < 10 or price == 0:
        return LevelAnalysisResult(
            [], [], price * 0.998, price * 1.002,
            0.2, 0.2, 65.0, 65.0, "Мало данных — уровни не определены"
        )

    high = df["high"]
    low  = df["low"]

    raw_res = _swing_highs(high, window=3)
    raw_sup = _swing_lows(low,  window=3)

    resistances = sorted([r for r in _cluster(raw_res) if r > price * 0.9995])
    supports    = sorted([s for s in _cluster(raw_sup) if s < price * 1.0005], reverse=True)

    if not resistances:
        resistances = [float(high.iloc[-20:].max())]
    if not supports:
        supports = [float(low.iloc[-20:].min())]

    nearest_res = resistances[0] if resistances else price * 1.005
    nearest_sup = supports[0]    if supports    else price * 0.995

    dist_res_pct = max(0.0, (nearest_res - price) / price * 100)
    dist_sup_pct = max(0.0, (price - nearest_sup) / price * 100)

    # ── Score: graduated penalty, not binary block ────────────────────────────
    # BUY headroom (resistance above): more room = better
    # Thresholds: HARD_BLOCK=0.08%, HEAVY_PENALTY=0.15%, LIGHT_PENALTY=0.30%, GOOD=0.50%
    buy_score  = _headroom_score(dist_res_pct)
    sell_score = _headroom_score(dist_sup_pct)

    parts = []
    if dist_res_pct < 0.15:
        parts.append(f"⚠️ Сопротивление близко ({dist_res_pct:.3f}%) — BUY ослаблен")
    else:
        parts.append(f"До сопротивления {dist_res_pct:.2f}%")

    if dist_sup_pct < 0.15:
        parts.append(f"⚠️ Поддержка близко ({dist_sup_pct:.3f}%) — SELL ослаблен")
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


def _headroom_score(dist_pct: float) -> float:
    """
    Convert distance-to-opposing-level percentage into a 0-100 score.
    Higher distance = higher score (more room to move = better).

    For 1-minute OTC binary options:
      < 0.02% = literally at the wall → hard block
      < 0.05% = very tight (1-2 pips at EUR/USD) → severe penalty
      < 0.15% = close → moderate penalty
      < 0.40% = acceptable
      >= 0.40% = good headroom
    """
    if dist_pct < 0.02:
        return 5.0     # hard block: price IS the level
    if dist_pct < 0.05:
        return 18.0    # extreme proximity
    if dist_pct < 0.15:
        return 38.0    # tight
    if dist_pct < 0.40:
        return 62.0    # moderate
    return 88.0        # good headroom
