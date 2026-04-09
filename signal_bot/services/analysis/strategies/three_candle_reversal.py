"""
Strategy 2 — Three Candles Same Color (Pattern Reversal)
Philosophy: OTC price generation creates frequent micro-reversals. A series of 3
consecutive same-color candles, each extending the move, is a statistically common
reversal point.

Entry:
  CALL: 3 consecutive bearish candles, each closing lower → reversal on 4th
  PUT:  3 consecutive bullish candles, each closing higher → reversal on 4th

Filters: no doji, similar candle sizes, not in a 5+ candle trend
Expiry: 1 minute
Best in: RANGE, SQUEEZE, VOLATILE
"""
from __future__ import annotations
try:
    from ..pair_profile import PairParams
except ImportError:
    PairParams = None  # type: ignore[misc,assignment]
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


_TOTAL   = 7
_MIN_MET = 5


def three_candle_reversal_strategy(
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

    if n < 7:
        return _none("Мало данных", {"early_reject": "n<7"})

    # Dead market guard
    if ind.atr_ratio < 0.25:
        return _none("ATR мёртвый", {"early_reject": f"atr_ratio={ind.atr_ratio:.3f}<0.25"})

    price = close[-1]
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    # Candles we examine: indices -4, -3, -2 (three pattern candles), -1 is current
    # c[-4]=candle1, c[-3]=candle2, c[-2]=candle3, c[-1]=current (4th)
    # We need at least 4 candles: the 3 pattern + current
    if n < 4:
        return _none("Мало данных для паттерна", {"early_reject": "n<4"})

    allow_4 = pair_params.allow_4_candles if pair_params else False
    buy_met, buy_parts, buy_conds = _check_buy(close, open_, high, low, n, avg_body, allow_4)
    sell_met, sell_parts, sell_conds = _check_sell(close, open_, high, low, n, avg_body, allow_4)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Условия не выполнены"
    dbg_conds = {}

    buy_wins  = buy_met > sell_met or (buy_met == sell_met and ctx_trend_down)
    sell_wins = sell_met > buy_met or (sell_met == buy_met and ctx_trend_up)

    if buy_wins and buy_met >= _MIN_MET:
        direction = "BUY"
        conditions_met = buy_met
        base_conf = 58 + (buy_met - _MIN_MET) * 10
        reason = " | ".join(buy_parts)
        dbg_conds = buy_conds
        # Strong: RSI was deeply oversold before reversal (extra confirmation)
        if ind.rsi < 35:
            base_conf += 5
            reason += f" | RSI {ind.rsi:.1f} перепроданность (+5)"

    elif sell_wins and sell_met >= _MIN_MET:
        direction = "SELL"
        conditions_met = sell_met
        base_conf = 58 + (sell_met - _MIN_MET) * 10
        reason = " | ".join(sell_parts)
        dbg_conds = sell_conds
        if ind.rsi > 65:
            base_conf += 5
            reason += f" | RSI {ind.rsi:.1f} перекупленность (+5)"

    # Block against strong trends (5+ candles in one direction = trend, not reversal)
    if direction == "BUY" and mode == "TRENDING_DOWN" and ctx_trend_down:
        return _none(
            "BUY заблокирован: сильный медвежий тренд",
            {"trend_block": True, "buy_met": buy_met, "sell_met": sell_met,
             "buy_conditions": buy_conds, "sell_conditions": sell_conds},
        )
    if direction == "SELL" and mode == "TRENDING_UP" and ctx_trend_up:
        return _none(
            "SELL заблокирован: сильный бычий тренд",
            {"trend_block": True, "buy_met": buy_met, "sell_met": sell_met,
             "buy_conditions": buy_conds, "sell_conditions": sell_conds},
        )

    if direction == "NONE":
        return _none(reason, {
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds, "sell_conditions": sell_conds,
        })

    return StrategyResult(
        direction=direction,
        confidence=max(0.0, min(100.0, base_conf)),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="three_candle_reversal",
        reasoning=reason,
        debug={
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds if direction == "BUY" else {},
            "sell_conditions": sell_conds if direction == "SELL" else {},
        }
    )


def _check_buy(close, open_, high, low, n, avg_body, allow_4_candles=False):
    """3 (or 4 for volatile pairs) consecutive bearish candles → expect bullish reversal."""
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # Need candles at positions -4, -3, -2 (three bearish), -1 is current, -5 is before series
    c1 = close[-4]; o1 = open_[-4]
    c2 = close[-3]; o2 = open_[-3]
    c3 = close[-2]; o3 = open_[-2]
    # "before series" candle — the one that should NOT be bearish
    c0 = close[-5] if n >= 5 else close[-4]
    o0 = open_[-5] if n >= 5 else open_[-4]

    # C1: All three candles are bearish (close < open)
    # For volatile pairs (allow_4_candles): also accept a 4-candle pattern
    bearish_1 = c1 < o1
    bearish_2 = c2 < o2
    bearish_3 = c3 < o3
    cond1 = bearish_1 and bearish_2 and bearish_3
    if not cond1 and allow_4_candles and n >= 5:
        # Check 4 consecutive bearish: -5, -4, -3, -2
        cond1 = (close[-5] < open_[-5] and bearish_1 and bearish_2 and bearish_3)
    conds["three_bearish"] = cond1
    if cond1:
        met += 1; parts.append("3+ медвежьих свечи подряд")

    # C2: Descending closes (each close lower than previous)
    cond2 = c2 < c1 and c3 < c2
    conds["descending_closes"] = cond2
    if cond2:
        met += 1; parts.append("Каждая свеча ниже предыдущей")

    # C3: Candle sizes similar — no one candle >3x bigger than average of the three
    bodies = [abs(c1 - o1), abs(c2 - o2), abs(c3 - o3)]
    if any(b > 0 for b in bodies):
        avg_of_three = float(np.mean([b for b in bodies if b > 0]))
        max_body = max(bodies)
        cond3 = avg_of_three > 0 and max_body < avg_of_three * 3.0
    else:
        avg_of_three = 0.0
        cond3 = False
    conds["similar_candle_sizes"] = cond3
    if cond3:
        met += 1; parts.append("Свечи похожего размера")

    # C4: No doji among the three (each body >= 20% of candle range)
    def not_doji(o, c, h, l):
        rng = h - l
        return rng > 0 and abs(c - o) / rng >= 0.20

    nd1 = not_doji(o1, c1, high[-4], low[-4])
    nd2 = not_doji(o2, c2, high[-3], low[-3])
    nd3 = not_doji(o3, c3, high[-2], low[-2])
    cond4 = nd1 and nd2 and nd3
    conds["no_doji"] = cond4
    if cond4:
        met += 1; parts.append("Нет дожи среди 3 свечей")

    # C5: Not more than 3 consecutive bearish before the pattern (no 5+ candle trend)
    streak = 0
    for i in range(5, min(10, n)):
        if close[-i] < open_[-i]:
            streak += 1
        else:
            break
    cond5 = streak < 2  # at most 2 extra bearish before the 3 we found
    conds["no_strong_trend"] = cond5
    if cond5:
        met += 1; parts.append("Не сильный тренд перед паттерном")

    # C6: Candle before series is NOT bearish (series is exactly 3, not 4+)
    cond6 = c0 >= o0  # c0 is bullish or neutral — series starts fresh
    conds["c0_not_same_color"] = cond6
    if cond6:
        met += 1; parts.append("Свеча до серии не медвежья (серия ровно 3)")

    # C7: No momentum spike before the series (body_before < avg_series * 3)
    body_before = abs(c0 - o0)
    cond7 = avg_of_three == 0 or body_before < avg_of_three * 3.0
    conds["no_impulse_before"] = cond7
    if cond7:
        met += 1; parts.append("Нет импульса до серии")

    return met, parts, conds


def _check_sell(close, open_, high, low, n, avg_body, allow_4_candles=False):
    """3 (or 4 for volatile pairs) consecutive bullish candles → expect bearish reversal."""
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    c1 = close[-4]; o1 = open_[-4]
    c2 = close[-3]; o2 = open_[-3]
    c3 = close[-2]; o3 = open_[-2]
    c0 = close[-5] if n >= 5 else close[-4]
    o0 = open_[-5] if n >= 5 else open_[-4]

    # C1: All three candles are bullish
    bullish_1 = c1 > o1
    bullish_2 = c2 > o2
    bullish_3 = c3 > o3
    cond1 = bullish_1 and bullish_2 and bullish_3
    if not cond1 and allow_4_candles and n >= 5:
        cond1 = (close[-5] > open_[-5] and bullish_1 and bullish_2 and bullish_3)
    conds["three_bullish"] = cond1
    if cond1:
        met += 1; parts.append("3+ бычьих свечи подряд")

    # C2: Ascending closes
    cond2 = c2 > c1 and c3 > c2
    conds["ascending_closes"] = cond2
    if cond2:
        met += 1; parts.append("Каждая свеча выше предыдущей")

    # C3: Similar candle sizes
    bodies = [abs(c1 - o1), abs(c2 - o2), abs(c3 - o3)]
    if any(b > 0 for b in bodies):
        avg_of_three = float(np.mean([b for b in bodies if b > 0]))
        max_body = max(bodies)
        cond3 = avg_of_three > 0 and max_body < avg_of_three * 3.0
    else:
        avg_of_three = 0.0
        cond3 = False
    conds["similar_candle_sizes"] = cond3
    if cond3:
        met += 1; parts.append("Свечи похожего размера")

    # C4: No doji
    def not_doji(o, c, h, l):
        rng = h - l
        return rng > 0 and abs(c - o) / rng >= 0.20

    cond4 = (not_doji(o1, c1, high[-4], low[-4]) and
             not_doji(o2, c2, high[-3], low[-3]) and
             not_doji(o3, c3, high[-2], low[-2]))
    conds["no_doji"] = cond4
    if cond4:
        met += 1; parts.append("Нет дожи среди 3 свечей")

    # C5: Not more than 3 consecutive bullish before the pattern
    streak = 0
    for i in range(5, min(10, n)):
        if close[-i] > open_[-i]:
            streak += 1
        else:
            break
    cond5 = streak < 2
    conds["no_strong_trend"] = cond5
    if cond5:
        met += 1; parts.append("Не сильный тренд перед паттерном")

    # C6: Candle before series is NOT bullish (series is exactly 3, not 4+)
    cond6 = c0 <= o0  # c0 is bearish or neutral
    conds["c0_not_same_color"] = cond6
    if cond6:
        met += 1; parts.append("Свеча до серии не бычья (серия ровно 3)")

    # C7: No momentum spike before the series
    body_before = abs(c0 - o0)
    cond7 = avg_of_three == 0 or body_before < avg_of_three * 3.0
    conds["no_impulse_before"] = cond7
    if cond7:
        met += 1; parts.append("Нет импульса до серии")

    return met, parts, conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "three_candle_reversal", reason, extra or {})
