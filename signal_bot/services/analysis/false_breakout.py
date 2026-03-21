"""
False Breakout Strategy — relaxed v3
Detects price briefly piercing a level then returning — the "trap" pattern.

Changes:
  - Uses level zones (± tolerance) instead of exact prices
  - Looks back 5 bars (was 4)
  - Relaxed confirmation: any bear/bull close counts
  - Partial score for wick pierces (not just close pierces)
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class FalseBreakoutResult:
    direction: str
    buy_score: float
    sell_score: float
    breakout_side: str
    recovery_strength: float
    explanation: str
    debug: dict


def false_breakout_strategy(
    df: pd.DataFrame,
    supports: list,
    resistances: list,
) -> FalseBreakoutResult:
    n = len(df)
    no_result = FalseBreakoutResult(
        "none", 0.0, 0.0, "none", 0.0,
        "Нет уровней для анализа",
        {}
    )

    if n < 5:
        return no_result

    cl    = df["close"].values
    op    = df["open"].values
    hi    = df["high"].values
    lo    = df["low"].values
    price = float(cl[-1])

    body_abs = np.abs(cl - op)
    avg_body = float(np.mean(body_abs[-min(10, n):])) or 1e-8
    avg_body_pct = avg_body / price if price > 0 else 0.001
    tolerance = max(0.002, avg_body_pct) * price

    buy_score  = 0.0
    sell_score = 0.0
    b_side     = "none"
    recovery   = 0.0
    parts      = []
    debug: dict = {"tolerance_pct": round(avg_body_pct * 100, 4)}

    lookback = min(5, n - 1)

    # ── False breakout DOWN → BUY ─────────────────────────────────────────────
    # Pattern: low went below support zone → now back above it
    for sup in sorted(supports, reverse=True)[:5]:
        zone_lo = sup - tolerance
        zone_hi = sup + tolerance

        # Any bar in lookback dipped into or below the zone
        dip_bar = -1
        dip_type = ""
        for i in range(1, lookback + 1):
            bar_lo  = float(lo[-i])
            bar_cl  = float(cl[-i])
            # Close below zone = strong pierce
            if bar_cl < zone_lo:
                dip_bar = i; dip_type = "close"; break
            # Wick below zone = weak pierce
            elif bar_lo < zone_lo:
                dip_bar = i; dip_type = "wick"; break

        if dip_bar < 0:
            continue

        # Current price must be back above zone_lo (recovery)
        if price < zone_lo:
            continue

        last_body  = float(cl[-1] - op[-1])
        body_ratio = abs(last_body) / avg_body

        score = 0.0
        # Recovery confirmed (back above zone)
        score += 35.0
        # Current bar is bullish
        if last_body > 0:
            score += 30.0
        elif last_body > -avg_body * 0.3:
            score += 10.0
        # Body strength
        if body_ratio >= 1.0:
            score += 20.0
        elif body_ratio >= 0.5:
            score += 10.0
        # Recency: more recent dip = stronger signal
        score += max(0.0, (lookback - dip_bar + 1) * 5.0)
        # Strong pierce (close below) is stronger signal
        if dip_type == "close":
            score += 10.0
        # Deduct if dip bar was more recent (2+ bars ago = weakening)
        if dip_bar > 3:
            score *= 0.8

        score = min(100.0, score)
        if score > buy_score:
            buy_score = score
            b_side    = "down"
            recovery  = body_ratio
            parts.append(
                f"Ложный пробой вниз sup={sup:.5f} ({dip_type}, {dip_bar}б назад, score {score:.0f})"
            )

    # ── False breakout UP → SELL ──────────────────────────────────────────────
    for res in sorted(resistances)[:5]:
        zone_lo = res - tolerance
        zone_hi = res + tolerance

        spike_bar = -1
        spike_type = ""
        for i in range(1, lookback + 1):
            bar_hi = float(hi[-i])
            bar_cl = float(cl[-i])
            if bar_cl > zone_hi:
                spike_bar = i; spike_type = "close"; break
            elif bar_hi > zone_hi:
                spike_bar = i; spike_type = "wick"; break

        if spike_bar < 0:
            continue

        if price > zone_hi:
            continue

        last_body  = float(cl[-1] - op[-1])
        body_ratio = abs(last_body) / avg_body

        score = 0.0
        score += 35.0
        if last_body < 0:
            score += 30.0
        elif last_body < avg_body * 0.3:
            score += 10.0
        if body_ratio >= 1.0:
            score += 20.0
        elif body_ratio >= 0.5:
            score += 10.0
        score += max(0.0, (lookback - spike_bar + 1) * 5.0)
        if spike_type == "close":
            score += 10.0
        if spike_bar > 3:
            score *= 0.8

        score = min(100.0, score)
        if score > sell_score:
            sell_score = score
            if b_side == "none":
                b_side = "up"
            recovery = max(recovery, body_ratio)
            parts.append(
                f"Ложный пробой вверх res={res:.5f} ({spike_type}, {spike_bar}б назад, score {score:.0f})"
            )

    direction = "none"
    if buy_score >= sell_score and buy_score >= 25:
        direction = "buy"
    elif sell_score > buy_score and sell_score >= 25:
        direction = "sell"

    return FalseBreakoutResult(
        direction=direction,
        buy_score=buy_score,
        sell_score=sell_score,
        breakout_side=b_side,
        recovery_strength=round(recovery, 2),
        explanation="; ".join(parts) if parts else "Ложного пробоя не обнаружено",
        debug=debug,
    )
