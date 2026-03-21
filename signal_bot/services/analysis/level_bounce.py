"""
Level Bounce Strategy
Detects price approaching and bouncing from support or resistance.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class LevelBounceResult:
    direction: str             # buy | sell | none
    buy_score: float           # 0-100
    sell_score: float          # 0-100
    detected_level_type: str   # support | resistance | none
    distance_to_next_level: float
    explanation: str


def level_bounce_strategy(
    df: pd.DataFrame,
    supports: list,
    resistances: list,
) -> LevelBounceResult:
    n = len(df)
    if n < 5 or (not supports and not resistances):
        return LevelBounceResult("none", 0.0, 0.0, "none", 0.0, "Нет уровней для анализа")

    price    = float(df["close"].iloc[-1])
    cl       = df["close"].values
    op       = df["open"].values
    hi       = df["high"].values
    lo       = df["low"].values
    body_abs = np.abs(cl - op)
    avg_body = float(np.mean(body_abs[-10:])) if n >= 10 else float(np.mean(body_abs))
    if avg_body == 0:
        avg_body = 1e-8

    PROXIMITY = 0.003   # 0.3% = "at level"

    buy_score  = 0.0
    sell_score = 0.0
    level_type = "none"
    dist_next  = 0.0
    parts      = []

    # ── Support bounce → BUY ─────────────────────────────────────────────────
    for sup in sorted(supports, reverse=True)[:3]:  # nearest supports
        dist_pct = (price - sup) / price
        if dist_pct < 0 or dist_pct > PROXIMITY * 3:
            continue  # not near support

        # Check recent candle shows rejection from below (lower shadow or bull close)
        last_body  = float(cl[-1] - op[-1])
        lower_wick = float(lo[-1]) - min(float(cl[-1]), float(op[-1]))
        upper_wick = max(float(cl[-1]), float(op[-1])) - float(hi[-1])

        rejection_score = 0.0
        if last_body > 0:                       # bullish close
            rejection_score += 40.0
        if lower_wick > avg_body * 0.5:         # meaningful lower wick (buyers stepping in)
            rejection_score += 30.0
        if dist_pct <= PROXIMITY:               # very close = stronger signal
            rejection_score += 20.0
        else:
            rejection_score += 5.0

        # Penalise if resistance too close above
        if resistances:
            nearest_res = min([r for r in resistances if r > price], default=price * 1.05)
            room_pct = (nearest_res - price) / price
            if room_pct < 0.002:
                rejection_score *= 0.3          # wall above — bad for BUY
            elif room_pct < 0.004:
                rejection_score *= 0.7

        buy_score  = max(buy_score, rejection_score)
        level_type = "support"
        dist_next  = dist_pct * 100
        parts.append(f"Отбой от поддержки {sup:.5f} (dist {dist_pct*100:.2f}%)")
        break

    # ── Resistance bounce → SELL ──────────────────────────────────────────────
    for res in sorted(resistances)[:3]:  # nearest resistances
        dist_pct = (res - price) / price
        if dist_pct < 0 or dist_pct > PROXIMITY * 3:
            continue  # not near resistance

        last_body  = float(cl[-1] - op[-1])
        upper_wick = max(float(cl[-1]), float(op[-1])) - float(hi[-1])
        lower_wick = float(lo[-1]) - min(float(cl[-1]), float(op[-1]))

        rejection_score = 0.0
        if last_body < 0:                       # bearish close
            rejection_score += 40.0
        if upper_wick > avg_body * 0.5:         # meaningful upper wick (sellers stepping in)
            rejection_score += 30.0
        if dist_pct <= PROXIMITY:
            rejection_score += 20.0
        else:
            rejection_score += 5.0

        # Penalise if support too close below
        if supports:
            nearest_sup = max([s for s in supports if s < price], default=price * 0.95)
            room_pct = (price - nearest_sup) / price
            if room_pct < 0.002:
                rejection_score *= 0.3
            elif room_pct < 0.004:
                rejection_score *= 0.7

        sell_score  = max(sell_score, rejection_score)
        if level_type == "none":
            level_type = "resistance"
        dist_next = dist_pct * 100
        parts.append(f"Отбой от сопротивления {res:.5f} (dist {dist_pct*100:.2f}%)")
        break

    direction = "none"
    if buy_score > sell_score and buy_score >= 35:
        direction = "buy"
    elif sell_score > buy_score and sell_score >= 35:
        direction = "sell"

    return LevelBounceResult(
        direction=direction,
        buy_score=buy_score,
        sell_score=sell_score,
        detected_level_type=level_type,
        distance_to_next_level=dist_next,
        explanation="; ".join(parts) if parts else "Нет отбоя от уровней",
    )
