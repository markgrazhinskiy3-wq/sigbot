"""
Strategy 2 — Squeeze Breakout
Scenario: Market compressed (small candles, low ATR, narrow BB).
A large impulse candle breaks out — enter in breakout direction.
Best in: SQUEEZE mode (primary) and TRENDING modes (secondary).
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


_TOTAL = 7


def squeeze_breakout_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
) -> StrategyResult:
    close = df["close"].values
    open_ = df["open"].values
    high  = df["high"].values
    low   = df["low"].values
    n     = len(df)

    if n < 15:
        return _none("Мало данных", {"early_reject": "n<15"})

    # Squeeze breakouts need real energy to follow through
    if ind.atr_ratio < 0.50:
        return _none("ATR слишком мал для пробоя",
                     {"early_reject": f"atr_ratio={round(ind.atr_ratio,3)}<0.50",
                      "atr_ratio": round(ind.atr_ratio, 3)})

    price    = close[-1]
    avg_body_30 = float(np.mean(np.abs(close[-min(30, n):] - open_[-min(30, n):]))) or 1e-8
    avg_body_10 = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8
    curr_body   = abs(close[-1] - open_[-1])

    # ── BUY / SELL check ───────────────────────────────────────────────────────
    buy_met, buy_parts, buy_conds   = _check_buy(close, open_, high, low, n, ind, avg_body_30, avg_body_10, curr_body, levels)
    sell_met, sell_parts, sell_conds = _check_sell(close, open_, high, low, n, ind, avg_body_30, avg_body_10, curr_body, levels)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Условия не выполнены"

    if buy_met > sell_met and buy_met >= 4:
        direction      = "BUY"
        conditions_met = buy_met
        base_conf      = buy_met / _TOTAL * 80
        reason         = " | ".join(buy_parts)
        # Squeeze duration bonus — check if last 8+ candles were all small
        small_run = sum(1 for i in range(1, min(9, n)) if abs(close[-i] - open_[-i]) < avg_body_30 * 0.6)
        if small_run >= 8:  base_conf += 5
        if close[-1] > ind.bb_upper:  base_conf += 5   # broke BB
        if ind.momentum > ind.momentum_prev * 2 and ind.momentum > 0:
            base_conf += 3

    elif sell_met > buy_met and sell_met >= 4:
        direction      = "SELL"
        conditions_met = sell_met
        base_conf      = sell_met / _TOTAL * 80
        reason         = " | ".join(sell_parts)
        small_run = sum(1 for i in range(1, min(9, n)) if abs(close[-i] - open_[-i]) < avg_body_30 * 0.6)
        if small_run >= 8:  base_conf += 5
        if close[-1] < ind.bb_lower:  base_conf += 5
        if ind.momentum < ind.momentum_prev * 2 and ind.momentum < 0:
            base_conf += 3

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="squeeze_breakout",
        reasoning=reason,
        debug={
            "buy_met": buy_met, "sell_met": sell_met,
            "avg_body_30": round(avg_body_30, 6),
            "buy_conditions": buy_conds,
            "sell_conditions": sell_conds,
        }
    )


def _check_buy(close, open_, high, low, n, ind: Indicators, avg_body_30, avg_body_10, curr_body, levels: LevelSet):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # 1. ATR compressed — relaxed: < 0.85× avg30 (was 0.70×)
    c1 = ind.atr_ratio < 0.85
    conds["atr_compressed"] = c1
    if c1:
        met += 1; parts.append(f"ATR сжат ×{ind.atr_ratio:.2f}")

    # 2. Small candles — relaxed: 4+ of 8 bodies < 75% of avg_30 (was 5+, threshold 60%)
    small_recent = sum(1 for i in range(1, min(9, n)) if abs(close[-i] - open_[-i]) < avg_body_30 * 0.75)
    c2 = small_recent >= 4
    conds["small_candles_compressed"] = c2
    if c2:
        met += 1; parts.append(f"Сжатые свечи ({small_recent} из последних 8)")

    # 3. Current candle body > 1.5× avg body of last 10 and bullish (relaxed from 2.0×)
    c3 = curr_body > avg_body_10 * 1.5 and close[-1] > open_[-1]
    conds["breakout_candle_bullish"] = c3
    if c3:
        met += 1; parts.append(f"Пробойная бычья свеча (×{curr_body/avg_body_10:.1f})")

    # 4. Close > upper BB or > highest high of last 8 candles
    high_8 = float(max(high[-min(9, n):-1])) if n >= 2 else float(high[-1])
    c4 = close[-1] > ind.bb_upper or close[-1] > high_8
    conds["breaks_bb_or_range"] = c4
    if c4:
        met += 1; parts.append("Пробой вверх (BB/диапазон)")

    # 5. Shadow against direction < 30% of candle range
    total_range = high[-1] - low[-1]
    upper_shadow = high[-1] - max(close[-1], open_[-1])
    c5 = total_range > 0 and upper_shadow / total_range < 0.3
    conds["small_upper_shadow"] = c5
    if c5:
        met += 1; parts.append("Малая верхняя тень")

    # 6. Momentum > 0 and positive
    c6 = ind.momentum > 0
    conds["momentum_positive"] = c6
    if c6:
        met += 1; parts.append(f"Моментум растёт ({ind.momentum:+.5f})")

    # 7. EMA(5) turning up or already > EMA(13)
    c7 = ind.ema5 >= ind.ema13 or ind.ema5 > float(ind.ema5_series.iloc[-2])
    conds["ema5_turning_up"] = c7
    if c7:
        met += 1; parts.append("EMA5 поворачивает вверх")

    # Hard reject: shadow > 60% of body or running into strong resistance (relaxed from 50%)
    upper_shadow_body = upper_shadow / (curr_body + 1e-10)
    if upper_shadow_body > 0.6:
        met = max(0, met - 2)   # heavy penalty
    if levels.dist_to_res_pct < 0.05:
        met = max(0, met - 2)
    if ind.rsi > 82:
        met = 0   # exhausted

    return met, parts, conds


def _check_sell(close, open_, high, low, n, ind: Indicators, avg_body_30, avg_body_10, curr_body, levels: LevelSet):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # 1. ATR compressed — relaxed: < 0.85× avg30 (was 0.70×)
    c1 = ind.atr_ratio < 0.85
    conds["atr_compressed"] = c1
    if c1:
        met += 1; parts.append(f"ATR сжат ×{ind.atr_ratio:.2f}")

    # 2. Small candles — relaxed: 4+ of 8 bodies < 75% of avg_30 (was 5+, threshold 60%)
    small_recent = sum(1 for i in range(1, min(9, n)) if abs(close[-i] - open_[-i]) < avg_body_30 * 0.75)
    c2 = small_recent >= 4
    conds["small_candles_compressed"] = c2
    if c2:
        met += 1; parts.append(f"Сжатые свечи ({small_recent})")

    # 3. Current candle body > 1.5× avg body of last 10 and bearish (relaxed from 2.0×)
    c3 = curr_body > avg_body_10 * 1.5 and close[-1] < open_[-1]
    conds["breakout_candle_bearish"] = c3
    if c3:
        met += 1; parts.append(f"Пробойная медвежья свеча (×{curr_body/avg_body_10:.1f})")

    low_8 = float(min(low[-min(9, n):-1])) if n >= 2 else float(low[-1])
    c4 = close[-1] < ind.bb_lower or close[-1] < low_8
    conds["breaks_bb_or_range"] = c4
    if c4:
        met += 1; parts.append("Пробой вниз (BB/диапазон)")

    total_range = high[-1] - low[-1]
    lower_shadow = min(close[-1], open_[-1]) - low[-1]
    c5 = total_range > 0 and lower_shadow / total_range < 0.3
    conds["small_lower_shadow"] = c5
    if c5:
        met += 1; parts.append("Малая нижняя тень")

    c6 = ind.momentum < 0
    conds["momentum_negative"] = c6
    if c6:
        met += 1; parts.append(f"Моментум падает ({ind.momentum:+.5f})")

    c7 = ind.ema5 <= ind.ema13 or ind.ema5 < float(ind.ema5_series.iloc[-2])
    conds["ema5_turning_down"] = c7
    if c7:
        met += 1; parts.append("EMA5 поворачивает вниз")

    lower_shadow_body = lower_shadow / (curr_body + 1e-10)
    if lower_shadow_body > 0.6:   # relaxed from 0.5
        met = max(0, met - 2)
    if levels.dist_to_sup_pct < 0.05:
        met = max(0, met - 2)
    if ind.rsi < 18:
        met = 0

    return met, parts, conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "squeeze_breakout", reason,
                          extra or {})
