"""
False Breakout Strategy
Detects price breaking a level then quickly returning — trap pattern.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class FalseBreakoutResult:
    direction: str          # buy | sell | none
    buy_score: float        # 0-100
    sell_score: float       # 0-100
    breakout_side: str      # up | down | none
    recovery_strength: float
    explanation: str


def false_breakout_strategy(
    df: pd.DataFrame,
    supports: list,
    resistances: list,
) -> FalseBreakoutResult:
    n = len(df)
    if n < 5 or (not supports and not resistances):
        return FalseBreakoutResult("none", 0.0, 0.0, "none", 0.0, "Нет уровней для анализа")

    cl  = df["close"].values
    op  = df["open"].values
    hi  = df["high"].values
    lo  = df["low"].values
    price = float(cl[-1])

    body_abs = np.abs(cl - op)
    avg_body = float(np.mean(body_abs[-10:])) if n >= 10 else float(np.mean(body_abs))
    if avg_body == 0:
        avg_body = 1e-8

    buy_score  = 0.0
    sell_score = 0.0
    b_side     = "none"
    recovery   = 0.0
    parts      = []

    # ── False breakout DOWN → BUY ─────────────────────────────────────────────
    # Pattern: price went below support (1-3 bars ago), now back above it
    for sup in sorted(supports, reverse=True)[:3]:
        # Check recent candles: one of them dipped below support
        lookback = min(4, n - 1)
        dipped_below = False
        dip_bar      = -1
        for i in range(1, lookback + 1):
            if float(lo[-i]) < sup and float(cl[-i]) < sup:
                dipped_below = True
                dip_bar = i
                break

        if not dipped_below:
            continue

        # Current price must be back above support
        if price <= sup * 1.0003:
            continue

        # Recovery candle: current bar closes above support with meaningful body
        last_body    = float(cl[-1] - op[-1])
        body_ratio   = abs(last_body) / avg_body
        is_bull      = last_body > 0

        rec_strength = 0.0
        if is_bull:
            rec_strength += 40.0
        if body_ratio >= 1.0:    # strong recovery candle
            rec_strength += 30.0
        elif body_ratio >= 0.5:
            rec_strength += 15.0
        # More recent the dip, stronger the signal
        rec_strength += max(0.0, (lookback - dip_bar + 1) * 7.0)

        score = min(100.0, rec_strength)
        if score > buy_score:
            buy_score = score
            b_side    = "down"
            recovery  = body_ratio
            parts.append(
                f"Ложный пробой вниз поддержки {sup:.5f} "
                f"({dip_bar} бар назад), возврат подтверждён"
            )
        break

    # ── False breakout UP → SELL ──────────────────────────────────────────────
    # Pattern: price went above resistance, now back below it
    for res in sorted(resistances)[:3]:
        lookback = min(4, n - 1)
        spiked_above = False
        spike_bar    = -1
        for i in range(1, lookback + 1):
            if float(hi[-i]) > res and float(cl[-i]) > res:
                spiked_above = True
                spike_bar    = i
                break

        if not spiked_above:
            continue

        # Current price must be back below resistance
        if price >= res * 0.9997:
            continue

        last_body  = float(cl[-1] - op[-1])
        body_ratio = abs(last_body) / avg_body
        is_bear    = last_body < 0

        rec_strength = 0.0
        if is_bear:
            rec_strength += 40.0
        if body_ratio >= 1.0:
            rec_strength += 30.0
        elif body_ratio >= 0.5:
            rec_strength += 15.0
        rec_strength += max(0.0, (lookback - spike_bar + 1) * 7.0)

        score = min(100.0, rec_strength)
        if score > sell_score:
            sell_score = score
            if b_side == "none":
                b_side = "up"
            recovery   = max(recovery, body_ratio)
            parts.append(
                f"Ложный пробой вверх сопротивления {res:.5f} "
                f"({spike_bar} бар назад), возврат подтверждён"
            )
        break

    direction = "none"
    if buy_score > sell_score and buy_score >= 35:
        direction = "buy"
    elif sell_score > buy_score and sell_score >= 35:
        direction = "sell"

    return FalseBreakoutResult(
        direction=direction,
        buy_score=buy_score,
        sell_score=sell_score,
        breakout_side=b_side,
        recovery_strength=round(recovery, 2),
        explanation="; ".join(parts) if parts else "Ложного пробоя не обнаружено",
    )
