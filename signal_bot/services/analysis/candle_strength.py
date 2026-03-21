"""
Candle Strength Analysis
Evaluates body/shadow ratios, directional bias, and noise.

Neutral flag is now only set for truly chaotic patterns.
A single weak candle or shadow lowers score but does not block.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class CandleStrengthResult:
    bullish_strength: float   # 0-100
    bearish_strength: float   # 0-100
    neutral_flag: bool        # True = candles are genuinely chaotic/indecisive
    buy_score: float          # 0-100
    sell_score: float         # 0-100
    explanation: str


def candle_strength_analysis(df: pd.DataFrame, lookback: int = 5) -> CandleStrengthResult:
    n = len(df)
    if n < lookback:
        return CandleStrengthResult(0.0, 0.0, False, 50.0, 50.0, "Мало свечей")

    recent = df.iloc[-lookback:].copy()
    op  = recent["open"].values
    cl  = recent["close"].values
    hi  = recent["high"].values
    lo  = recent["low"].values

    body        = cl - op           # signed: positive = bull
    body_abs    = np.abs(body)
    total_range = hi - lo
    avg_range   = float(np.mean(total_range)) if float(np.mean(total_range)) > 0 else 1e-8
    avg_body    = float(np.mean(body_abs)) if float(np.mean(body_abs)) > 0 else 1e-8

    body_ratio = body_abs / (total_range + 1e-10)  # 0=doji, 1=full candle

    upper_shadow = hi - np.maximum(op, cl)
    lower_shadow = np.minimum(op, cl) - lo

    is_bull = body > 0
    is_bear = body < 0
    is_doji = body_abs < (avg_range * 0.08)   # very small body

    bull_count = int(np.sum(is_bull))
    bear_count = int(np.sum(is_bear))
    doji_count = int(np.sum(is_doji))

    bull_bodies = body_ratio[is_bull]
    bear_bodies = body_ratio[is_bear]
    bull_strength = float(np.mean(bull_bodies) * 100) if len(bull_bodies) else 0.0
    bear_strength = float(np.mean(bear_bodies) * 100) if len(bear_bodies) else 0.0

    avg_upper = float(np.mean(upper_shadow))
    avg_lower = float(np.mean(lower_shadow))
    upper_dom = avg_upper / avg_body  # > 1.5 = notable selling pressure
    lower_dom = avg_lower / avg_body  # > 1.5 = notable buying pressure

    # ── Neutral flag: only truly chaotic / all-doji patterns ─────────────────
    # (was too aggressive before — now requires multiple conditions)
    truly_chaotic = (
        doji_count >= lookback - 1                           # nearly all dojis
        or (doji_count >= 2 and bull_count > 0 and bear_count > 0
            and abs(bull_count - bear_count) <= 1)           # mixed + dojis
        or (float(np.mean(body_ratio)) < 0.12               # all bodies tiny
            and bull_count > 0 and bear_count > 0)           # and mixed
    )

    # ── Scores ────────────────────────────────────────────────────────────────
    # BUY score
    buy_score = 50.0   # start neutral (not 0)
    if bull_count >= 3:
        buy_score += 30.0
    elif bull_count == 2:
        buy_score += 15.0
    else:
        buy_score -= 10.0

    buy_score += bull_strength * 0.2

    if lower_dom > 1.5:       # buying wicks below = support / buyers
        buy_score += 12.0
    if upper_dom > 1.5:       # selling wicks above = resistance / sellers
        buy_score -= 15.0     # penalty, not a block

    buy_score = max(0.0, min(100.0, buy_score))

    # SELL score
    sell_score = 50.0
    if bear_count >= 3:
        sell_score += 30.0
    elif bear_count == 2:
        sell_score += 15.0
    else:
        sell_score -= 10.0

    sell_score += bear_strength * 0.2

    if upper_dom > 1.5:       # selling wicks = sellers active
        sell_score += 12.0
    if lower_dom > 1.5:       # buying wicks = buyers protecting
        sell_score -= 15.0

    sell_score = max(0.0, min(100.0, sell_score))

    # Mild penalty for truly chaotic candles (not zero-out)
    if truly_chaotic:
        buy_score  *= 0.55
        sell_score *= 0.55

    parts = [f"Бычьих {bull_count}/{lookback}, медвежьих {bear_count}/{lookback}, дожи {doji_count}"]
    if upper_dom > 1.5:
        parts.append("Верхние тени (продавцы)")
    if lower_dom > 1.5:
        parts.append("Нижние тени (покупатели)")
    if truly_chaotic:
        parts.append("Свечи хаотичны — confidence снижен")

    return CandleStrengthResult(
        bullish_strength=round(bull_strength, 1),
        bearish_strength=round(bear_strength, 1),
        neutral_flag=truly_chaotic,
        buy_score=round(buy_score, 1),
        sell_score=round(sell_score, 1),
        explanation="; ".join(parts),
    )
