"""
Strategy 6 — Double Bottom / Double Top OTC
Philosophy: Classic Price Action pattern. When price tests the same level twice
and fails to break through, a reversal is likely. 2-minute expiry gives time to develop.

Entry:
  CALL: Double bottom (W-pattern) — two lows at similar levels near BB lower, 
        second low doesn't break first, then price bounces up
  PUT:  Double top (M-pattern) — two highs at similar levels near BB upper,
        second high doesn't exceed first, then price drops

Expiry: 2 minutes
Best in: RANGE, SQUEEZE
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet


@dataclass
class StrategyResult:
    direction: str
    confidence: float
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL   = 5
_MIN_MET = 4


def double_bottom_top_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
    pair_params=None,
) -> StrategyResult:
    close = df["close"].values
    open_ = df["open"].values
    high  = df["high"].values
    low   = df["low"].values
    n     = len(df)

    if n < 20:
        return _none("Мало данных (нужно ≥20)", {"early_reject": "n<20"})

    if ind.atr_ratio < 0.25:
        return _none("ATR мёртвый", {"early_reject": f"atr_ratio={ind.atr_ratio:.3f}<0.25"})

    # Block in strong trends — double bottom/top works in range/reversal context
    if mode in ("TRENDING_UP", "TRENDING_DOWN"):
        close_s = pd.Series(close)
        ema5 = float(close_s.ewm(span=5, adjust=False).mean().iloc[-1])
        ema21 = float(close_s.ewm(span=21, adjust=False).mean().iloc[-1])
        spread_pct = abs(ema5 - ema21) / close[-1] if close[-1] > 0 else 0
        if spread_pct > 0.01:  # 1% spread = strongly trending
            return _none(
                "TRENDING с большим EMA-спредом: двойное дно/вершина ненадёжно",
                {"early_reject": f"strong_trend, spread={spread_pct:.4f}"},
            )

    # Compute BB(20, 2.0) inline
    close_s = pd.Series(close)
    bb_p = min(20, n)
    bb_mid_s = close_s.rolling(bb_p).mean()
    bb_std_s = close_s.rolling(bb_p).std()
    bb_upper = float((bb_mid_s + 2.0 * bb_std_s).iloc[-1])
    bb_lower = float((bb_mid_s - 2.0 * bb_std_s).iloc[-1])
    bb_mid   = float(bb_mid_s.iloc[-1])

    # Find local lows and highs in the last 25 bars
    scan_n = min(25, n)
    local_lows  = _find_local_lows(low[-scan_n:], scan_n)
    local_highs = _find_local_highs(high[-scan_n:], scan_n)

    buy_met, buy_parts, buy_conds = _check_double_bottom(
        close, open_, high, low, n, ind, bb_lower, bb_mid, bb_upper, local_lows, scan_n
    )
    sell_met, sell_parts, sell_conds = _check_double_top(
        close, open_, high, low, n, ind, bb_lower, bb_mid, bb_upper, local_highs, scan_n
    )

    buy_wins  = buy_met > sell_met or (buy_met == sell_met and ctx_trend_down)
    sell_wins = sell_met > buy_met or (sell_met == buy_met and ctx_trend_up)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Условия не выполнены"

    if buy_wins and buy_met >= _MIN_MET:
        direction = "BUY"
        conditions_met = buy_met
        base_conf = 62 + (buy_met - _MIN_MET) * 12
        reason = " | ".join(buy_parts)
        # RSI bonus: oversold at double bottom = perfect signal
        if ind.rsi < 35:
            base_conf += 8
            reason += f" | RSI {ind.rsi:.1f} подтверждает перепроданность (+8)"

    elif sell_wins and sell_met >= _MIN_MET:
        direction = "SELL"
        conditions_met = sell_met
        base_conf = 62 + (sell_met - _MIN_MET) * 12
        reason = " | ".join(sell_parts)
        if ind.rsi > 65:
            base_conf += 8
            reason += f" | RSI {ind.rsi:.1f} подтверждает перекупленность (+8)"

    if direction == "NONE":
        return _none(reason, {
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds, "sell_conditions": sell_conds,
            "bb_lower": round(bb_lower, 6), "bb_upper": round(bb_upper, 6),
            "local_lows_count": len(local_lows),
            "local_highs_count": len(local_highs),
        })

    return StrategyResult(
        direction=direction,
        confidence=max(0.0, min(100.0, base_conf)),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="double_bottom_top",
        reasoning=reason,
        debug={
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds if direction == "BUY" else {},
            "sell_conditions": sell_conds if direction == "SELL" else {},
            "bb_lower": round(bb_lower, 6), "bb_upper": round(bb_upper, 6),
            "expiry": "2m",
        }
    )


def _find_local_lows(lows: np.ndarray, n: int) -> list[tuple[int, float]]:
    """Find local lows: index where both neighbors have higher lows."""
    result = []
    for i in range(1, n - 1):
        if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
            result.append((i, float(lows[i])))
    return result


def _find_local_highs(highs: np.ndarray, n: int) -> list[tuple[int, float]]:
    """Find local highs: index where both neighbors have lower highs."""
    result = []
    for i in range(1, n - 1):
        if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
            result.append((i, float(highs[i])))
    return result


def _check_double_bottom(close, open_, high, low, n, ind,
                          bb_lower, bb_mid, bb_upper, local_lows, scan_n):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # Need at least 2 local lows
    if len(local_lows) < 2:
        conds["two_lows_found"] = False
        return met, parts, conds

    # Find the two most recent lows that form a W pattern
    # They must have at least 3 bars between them
    low1_idx, low1_val = None, None
    low2_idx, low2_val = None, None
    for i in range(len(local_lows) - 1, -1, -1):
        idx, val = local_lows[i]
        if low2_idx is None and idx < scan_n - 1:
            low2_idx, low2_val = idx, val
        elif low1_idx is None and idx < low2_idx - 2:
            low1_idx, low1_val = idx, val
            break

    if low1_idx is None:
        conds["two_lows_found"] = False
        return met, parts, conds

    # C1: Two local lows found with space between them
    c1 = True
    conds["two_lows_found"] = c1
    if c1:
        met += 1; parts.append(f"Два локальных дна: {low1_val:.6f} и {low2_val:.6f}")

    # C2: Second low is NOT significantly below first (within tolerance of 0.1%)
    tolerance = low1_val * 0.001  # 0.1%
    c2 = low2_val >= low1_val - tolerance
    conds["second_low_not_lower"] = c2
    if c2:
        met += 1; parts.append("Второе дно не пробило первое")

    # C3: At least one low touched BB lower band (within 0.2% of BB lower)
    bb_touch_pct = 0.002
    low1_near_bb = abs(low1_val - bb_lower) / bb_lower < bb_touch_pct
    low2_near_bb = abs(low2_val - bb_lower) / bb_lower < bb_touch_pct
    c3 = low1_near_bb or low2_near_bb
    conds["low_near_bb_lower"] = c3
    if c3:
        met += 1; parts.append(f"Дно у нижней BB ({bb_lower:.6f})")

    # C4: Recovery between lows: peak between them is at least 30% of BB range
    bb_range = bb_upper - bb_lower if bb_upper > bb_lower else 1e-8
    if low2_idx > low1_idx:
        between_highs = high[-scan_n + low1_idx: -scan_n + low2_idx + 1] if low2_idx < scan_n else high[low1_idx:]
        if len(between_highs) > 0:
            peak_between = float(np.max(between_highs))
            recovery_pct = (peak_between - min(low1_val, low2_val)) / bb_range
            c4 = recovery_pct >= 0.30
        else:
            c4 = False
    else:
        c4 = False
    conds["recovery_between_lows"] = c4
    if c4:
        met += 1; parts.append(f"Отскок между днами ≥30% диапазона BB")

    # C5: Current price is above the second low and moving up (confirmation)
    c5 = close[-1] > low2_val and close[-1] > open_[-1]
    conds["price_bouncing"] = c5
    if c5:
        met += 1; parts.append("Цена отскакивает от второго дна")

    return met, parts, conds


def _check_double_top(close, open_, high, low, n, ind,
                       bb_lower, bb_mid, bb_upper, local_highs, scan_n):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    if len(local_highs) < 2:
        conds["two_highs_found"] = False
        return met, parts, conds

    # Find two most recent highs with space between them
    high1_idx, high1_val = None, None
    high2_idx, high2_val = None, None
    for i in range(len(local_highs) - 1, -1, -1):
        idx, val = local_highs[i]
        if high2_idx is None and idx < scan_n - 1:
            high2_idx, high2_val = idx, val
        elif high1_idx is None and idx < high2_idx - 2:
            high1_idx, high1_val = idx, val
            break

    if high1_idx is None:
        conds["two_highs_found"] = False
        return met, parts, conds

    # C1: Two local highs found
    c1 = True
    conds["two_highs_found"] = c1
    if c1:
        met += 1; parts.append(f"Две локальных вершины: {high1_val:.6f} и {high2_val:.6f}")

    # C2: Second high is NOT significantly above first (within 0.1%)
    tolerance = high1_val * 0.001
    c2 = high2_val <= high1_val + tolerance
    conds["second_high_not_higher"] = c2
    if c2:
        met += 1; parts.append("Вторая вершина не превысила первую")

    # C3: At least one high touched BB upper band (within 0.2%)
    bb_touch_pct = 0.002
    high1_near_bb = abs(high1_val - bb_upper) / bb_upper < bb_touch_pct
    high2_near_bb = abs(high2_val - bb_upper) / bb_upper < bb_touch_pct
    c3 = high1_near_bb or high2_near_bb
    conds["high_near_bb_upper"] = c3
    if c3:
        met += 1; parts.append(f"Вершина у верхней BB ({bb_upper:.6f})")

    # C4: Pullback between highs: trough at least 30% of BB range below top
    bb_range = bb_upper - bb_lower if bb_upper > bb_lower else 1e-8
    if high2_idx > high1_idx:
        between_lows = low[-scan_n + high1_idx: -scan_n + high2_idx + 1] if high2_idx < scan_n else low[high1_idx:]
        if len(between_lows) > 0:
            trough = float(np.min(between_lows))
            pullback_pct = (max(high1_val, high2_val) - trough) / bb_range
            c4 = pullback_pct >= 0.30
        else:
            c4 = False
    else:
        c4 = False
    conds["pullback_between_highs"] = c4
    if c4:
        met += 1; parts.append("Откат между вершинами ≥30% диапазона BB")

    # C5: Current price is below second high and moving down (confirmation)
    c5 = close[-1] < high2_val and close[-1] < open_[-1]
    conds["price_dropping"] = c5
    if c5:
        met += 1; parts.append("Цена падает от второй вершины")

    return met, parts, conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "double_bottom_top", reason, extra or {})
