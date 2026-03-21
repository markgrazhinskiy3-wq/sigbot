"""
Level Bounce Strategy — relaxed v3
Detects price reaching a support/resistance zone and showing rejection.

Changes:
  - Uses zones (± tolerance) instead of exact levels
  - Tolerance = max(0.2%, 1x avg_body_pct)
  - Reduced minimum rejection score from 35 to 25
  - Checks last 3 bars for zone touch (not just last 1)
  - Partial match if price is near zone even without strong rejection candle
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class LevelBounceResult:
    direction: str
    buy_score: float
    sell_score: float
    detected_level_type: str
    distance_to_next_level: float
    explanation: str
    debug: dict


def level_bounce_strategy(
    df: pd.DataFrame,
    supports: list,
    resistances: list,
) -> LevelBounceResult:
    n = len(df)
    no_result = LevelBounceResult(
        "none", 0.0, 0.0, "none", 0.0,
        "Нет уровней для анализа",
        {"supports": [], "resistances": [], "tolerance_pct": 0}
    )

    if n < 4:
        return no_result

    price    = float(df["close"].iloc[-1])
    cl       = df["close"].values
    op       = df["open"].values
    hi       = df["high"].values
    lo       = df["low"].values

    body_abs = np.abs(cl - op)
    avg_body = float(np.mean(body_abs[-min(10, n):])) or 1e-8
    avg_body_pct = avg_body / price if price > 0 else 0.001

    # Zone tolerance: at least 0.2% or 1× avg body
    tolerance = max(0.002, avg_body_pct) * price

    buy_score  = 0.0
    sell_score = 0.0
    level_type = "none"
    dist_next  = 0.0
    parts      = []

    debug = {
        "price": round(price, 6),
        "tolerance_pct": round(avg_body_pct * 100, 4),
        "supports_checked": [],
        "resistances_checked": [],
    }

    # ── Support zone → BUY ───────────────────────────────────────────────────
    for sup in sorted(supports, reverse=True)[:5]:
        zone_lo = sup - tolerance
        zone_hi = sup + tolerance

        # Did price touch the zone in the last 3 bars?
        touched = False
        touch_bar = -1
        for i in range(1, min(4, n)):
            if float(lo[-i]) <= zone_hi and float(hi[-i]) >= zone_lo:
                touched = True
                touch_bar = i
                break

        dist_from_zone = max(0.0, (price - zone_hi) / price * 100)
        debug["supports_checked"].append({
            "level": round(sup, 6),
            "touched": touched,
            "dist_pct": round(dist_from_zone, 4),
        })

        if not touched and dist_from_zone > 0.5:
            continue  # too far from zone

        # Evaluate rejection quality from the LAST bar
        last_body  = float(cl[-1] - op[-1])
        lower_wick = min(float(cl[-1]), float(op[-1])) - float(lo[-1])
        score = 0.0

        # Price currently in or just above zone
        if price >= zone_lo and price <= zone_hi * 1.003:
            score += 35.0  # at the zone

        # Bull close
        if last_body > 0:
            score += 30.0
        elif last_body > -avg_body * 0.3:
            score += 10.0  # neutral but not strongly bearish

        # Lower wick rejection
        if lower_wick > avg_body * 0.3:
            score += 20.0
        elif lower_wick > 0:
            score += 8.0

        # Recent touch bonus
        if touch_bar == 1:
            score += 10.0
        elif touch_bar == 2:
            score += 5.0

        # Penalise if resistance too close above
        if resistances:
            above = [r for r in resistances if r > price]
            if above:
                room_pct = (min(above) - price) / price
                if room_pct < 0.001:
                    score *= 0.4
                elif room_pct < 0.003:
                    score *= 0.7

        score = min(100.0, score)
        if score > buy_score:
            buy_score  = score
            level_type = "support"
            dist_next  = dist_from_zone
            parts.append(f"Поддержка {sup:.5f} (score {score:.0f})")

    # ── Resistance zone → SELL ───────────────────────────────────────────────
    for res in sorted(resistances)[:5]:
        zone_lo = res - tolerance
        zone_hi = res + tolerance

        touched = False
        touch_bar = -1
        for i in range(1, min(4, n)):
            if float(lo[-i]) <= zone_hi and float(hi[-i]) >= zone_lo:
                touched = True
                touch_bar = i
                break

        dist_from_zone = max(0.0, (zone_lo - price) / price * 100)
        debug["resistances_checked"].append({
            "level": round(res, 6),
            "touched": touched,
            "dist_pct": round(dist_from_zone, 4),
        })

        if not touched and dist_from_zone > 0.5:
            continue

        last_body  = float(cl[-1] - op[-1])
        upper_wick = float(hi[-1]) - max(float(cl[-1]), float(op[-1]))
        score = 0.0

        if price >= zone_lo * 0.997 and price <= zone_hi:
            score += 35.0

        if last_body < 0:
            score += 30.0
        elif last_body < avg_body * 0.3:
            score += 10.0

        if upper_wick > avg_body * 0.3:
            score += 20.0
        elif upper_wick > 0:
            score += 8.0

        if touch_bar == 1:
            score += 10.0
        elif touch_bar == 2:
            score += 5.0

        if supports:
            below = [s for s in supports if s < price]
            if below:
                room_pct = (price - max(below)) / price
                if room_pct < 0.001:
                    score *= 0.4
                elif room_pct < 0.003:
                    score *= 0.7

        score = min(100.0, score)
        if score > sell_score:
            sell_score  = score
            if level_type == "none":
                level_type = "resistance"
            dist_next = dist_from_zone
            parts.append(f"Сопротивление {res:.5f} (score {score:.0f})")

    direction = "none"
    if buy_score >= sell_score and buy_score >= 25:
        direction = "buy"
    elif sell_score > buy_score and sell_score >= 25:
        direction = "sell"

    return LevelBounceResult(
        direction=direction,
        buy_score=buy_score,
        sell_score=sell_score,
        detected_level_type=level_type,
        distance_to_next_level=dist_next,
        explanation="; ".join(parts) if parts else "Нет касания зон уровней",
        debug=debug,
    )
