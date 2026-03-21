"""
Candle Strength Analysis
Evaluates body/shadow ratios, directional momentum, and noise.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class CandleStrengthResult:
    bullish_strength: float   # 0-100
    bearish_strength: float   # 0-100
    neutral_flag: bool        # True = too noisy / indecisive
    buy_score: float          # 0-100
    sell_score: float         # 0-100
    explanation: str


def candle_strength_analysis(df: pd.DataFrame, lookback: int = 5) -> CandleStrengthResult:
    n = len(df)
    if n < lookback:
        return CandleStrengthResult(0, 0, True, 0, 0, "Недостаточно свечей")

    recent = df.iloc[-lookback:].copy()
    op  = recent["open"].values
    cl  = recent["close"].values
    hi  = recent["high"].values
    lo  = recent["low"].values

    body        = np.abs(cl - op)
    total_range = hi - lo
    avg_range   = float(np.mean(total_range)) if float(np.mean(total_range)) > 0 else 1e-8

    upper_shadow = hi - np.maximum(op, cl)
    lower_shadow = np.minimum(op, cl) - lo
    body_ratio   = body / (total_range + 1e-10)   # 0 = doji, 1 = full body

    # Direction per candle
    is_bull = cl > op
    is_bear = cl < op
    is_doji = body < (avg_range * 0.1)

    # ── Bullish strength: body ratio of bullish candles ───────────────────────
    bull_bodies = body_ratio[is_bull]
    bear_bodies = body_ratio[is_bear]
    bull_strength = float(np.mean(bull_bodies) * 100) if len(bull_bodies) else 0.0
    bear_strength = float(np.mean(bear_bodies) * 100) if len(bear_bodies) else 0.0

    bull_count = int(np.sum(is_bull))
    bear_count = int(np.sum(is_bear))
    doji_count = int(np.sum(is_doji))

    # ── Long shadows against proposed entry ───────────────────────────────────
    # Upper shadow large → selling pressure → bad for BUY
    # Lower shadow large → buying pressure  → bad for SELL
    avg_upper = float(np.mean(upper_shadow))
    avg_lower = float(np.mean(lower_shadow))
    avg_body  = float(np.mean(body)) if float(np.mean(body)) > 0 else 1e-8

    upper_shadow_dominance = avg_upper / avg_body  # >1.5 = strong selling wicks
    lower_shadow_dominance = avg_lower / avg_body  # >1.5 = strong buying wicks

    # ── Neutral flag ─────────────────────────────────────────────────────────
    neutral_flag = (
        doji_count >= lookback // 2                   # mostly dojis
        or (bull_count > 0 and bear_count > 0
            and abs(bull_count - bear_count) <= 1
            and doji_count > 0)                        # mixed with indecision
        or float(np.mean(body_ratio)) < 0.2           # all bodies tiny
    )

    # ── Scores ───────────────────────────────────────────────────────────────
    # BUY score: boosted by bull count, bull strength, lower shadows; penalised by upper shadows
    buy_score = 0.0
    if bull_count >= 3:
        buy_score += 40.0
    elif bull_count == 2:
        buy_score += 20.0
    buy_score += bull_strength * 0.3
    if lower_shadow_dominance > 1.5:   # strong wicks below = buyers defending
        buy_score += 15.0
    if upper_shadow_dominance > 1.5:   # wicks above = sellers blocking
        buy_score -= 20.0
    buy_score = max(0.0, min(100.0, buy_score))

    # SELL score: boosted by bear count, bear strength, upper shadows; penalised by lower shadows
    sell_score = 0.0
    if bear_count >= 3:
        sell_score += 40.0
    elif bear_count == 2:
        sell_score += 20.0
    sell_score += bear_strength * 0.3
    if upper_shadow_dominance > 1.5:   # strong wicks above = sellers active
        sell_score += 15.0
    if lower_shadow_dominance > 1.5:   # wicks below = buyers present
        sell_score -= 20.0
    sell_score = max(0.0, min(100.0, sell_score))

    if neutral_flag:
        buy_score  *= 0.4
        sell_score *= 0.4

    parts = [f"Бычьих {bull_count}/{lookback}, медвежьих {bear_count}/{lookback}, дожи {doji_count}"]
    if upper_shadow_dominance > 1.5:
        parts.append("Длинные верхние тени (давление продавцов)")
    if lower_shadow_dominance > 1.5:
        parts.append("Длинные нижние тени (давление покупателей)")
    if neutral_flag:
        parts.append("Свечи неопределённые — сигнал ослаблен")

    return CandleStrengthResult(
        bullish_strength=round(bull_strength, 1),
        bearish_strength=round(bear_strength, 1),
        neutral_flag=neutral_flag,
        buy_score=round(buy_score, 1),
        sell_score=round(sell_score, 1),
        explanation="; ".join(parts),
    )
