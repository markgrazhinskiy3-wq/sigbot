"""
Strategy 5 — Micro Level Breakout
Scenario: Price tested a level 2-3 times and finally breaks through with a strong candle.
Breakout momentum carries 2-5 more candles.
Best in: VOLATILE mode and RANGE mode (when a level breaks).
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


def micro_breakout_strategy(
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

    if n < 10:
        return _none("Мало данных")

    # Hard reject: micro breakouts need strong VOLATILE conditions
    if ind.atr_ratio < 0.55:
        return _none("ATR недостаточный для микропробоя")

    avg_body_10 = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    buy_met, buy_conf, buy_parts, buy_conds = _check_buy(close, open_, high, low, n, ind, levels, avg_body_10, ctx_trend_up)
    sell_met, sell_conf, sell_parts, sell_conds = _check_sell(close, open_, high, low, n, ind, levels, avg_body_10, ctx_trend_down)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Уровень не пробит"

    if buy_conf > sell_conf and buy_met >= 4:
        direction = "BUY"; conditions_met = buy_met; base_conf = buy_conf
        reason = " | ".join(buy_parts)
    elif sell_conf > buy_conf and sell_met >= 4:
        direction = "SELL"; conditions_met = sell_met; base_conf = sell_conf
        reason = " | ".join(sell_parts)

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="micro_breakout",
        reasoning=reason,
        debug={
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds,
            "sell_conditions": sell_conds,
        }
    )


def _check_buy(close, open_, high, low, n, ind: Indicators, levels: LevelSet, avg_body_10, ctx_up):
    """Breakout of resistance → BUY."""
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # Find a resistance level with 2+ touches that current candle broke
    broken_res = None
    touch_count = 0
    for res in levels.strong_res:
        if close[-1] > res:   # closed above resistance
            broken_res = res
            touch_count = 2
            break

    conds["resistance_found"] = broken_res is not None
    if broken_res is None:
        return 0, 0.0, [], conds

    met += 1; parts.append(f"Сопротивление {broken_res:.5f} найдено (2+ касания)")
    conds["closed_above_resistance"] = True

    # 2. Current candle closed ABOVE resistance (always true if broken_res found)
    met += 1; parts.append(f"Пробой: закрылась выше {broken_res:.5f}")

    curr_body = abs(close[-1] - open_[-1])
    upper_shadow = high[-1] - max(close[-1], open_[-1])

    # 3. Breakout candle body > 1.5× avg body
    c3 = curr_body > avg_body_10 * 1.5
    conds["strong_body"] = c3
    if c3:
        met += 1; parts.append(f"Мощное тело ×{curr_body/avg_body_10:.1f}")

    # 4. Shadow against direction < 30% of body
    c4 = upper_shadow / (curr_body + 1e-10) < 0.3
    conds["small_upper_shadow"] = c4
    if c4:
        met += 1; parts.append("Малая верхняя тень")

    # 5. Momentum > 0
    c5 = ind.momentum > 0
    conds["momentum_positive"] = c5
    if c5:
        met += 1; parts.append("Моментум положительный")

    # 6. ATR active
    c6 = ind.atr_ratio > 0.9
    conds["atr_active"] = c6
    if c6:
        met += 1; parts.append(f"ATR активный ×{ind.atr_ratio:.2f}")

    # 7. EMA pointing up
    c7a = ind.ema5 >= ind.ema13 and ind.ema13 >= ind.ema21
    c7b = ind.ema5 > float(ind.ema5_series.iloc[-3])
    conds["ema_pointing_up"] = c7a or c7b
    if c7a:
        met += 1; parts.append("EMA вверх")
    elif c7b:
        met += 1; parts.append("EMA5 поворачивает вверх")

    # Penalties (applied before base_conf calculation)
    another_res = [r for r in levels.resistances if r > broken_res and (r - close[-1]) / close[-1] < 0.0005]
    conds["shadow_penalty"] = upper_shadow / (curr_body + 1e-10) > 0.5
    conds["wall_above"] = bool(another_res)
    conds["atr_weak"] = ind.atr_ratio < 0.6

    if conds["shadow_penalty"]:
        met = max(0, met - 2)
    if conds["wall_above"]:
        met = max(0, met - 2)
    if conds["atr_weak"]:
        met = max(0, met - 1)

    # base_conf calculated AFTER penalties
    base_conf = met / _TOTAL * 80

    # Bonuses
    if touch_count >= 3:
        base_conf += 5
    if (close[-1] - broken_res) / broken_res > 0.0005:
        base_conf += 3

    # Confidence penalty multipliers (applied after base_conf)
    if conds["shadow_penalty"]:
        base_conf *= 0.6
    if conds["wall_above"]:
        base_conf *= 0.6

    return met, base_conf, parts, conds


def _check_sell(close, open_, high, low, n, ind: Indicators, levels: LevelSet, avg_body_10, ctx_down):
    """Breakout of support → SELL."""
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    broken_sup = None
    touch_count = 0
    for sup in levels.strong_sup:
        if close[-1] < sup:   # closed below support
            broken_sup = sup
            touch_count = 2
            break

    conds["support_found"] = broken_sup is not None
    if broken_sup is None:
        return 0, 0.0, [], conds

    met += 1; parts.append(f"Поддержка {broken_sup:.5f} найдена (2+ касания)")
    conds["closed_below_support"] = True

    # 2. Current candle closed BELOW support (always true if broken_sup found)
    met += 1; parts.append(f"Пробой: закрылась ниже {broken_sup:.5f}")

    curr_body = abs(close[-1] - open_[-1])
    lower_shadow = min(close[-1], open_[-1]) - low[-1]

    # 3. Breakout candle body > 1.5× avg body
    c3 = curr_body > avg_body_10 * 1.5
    conds["strong_body"] = c3
    if c3:
        met += 1; parts.append(f"Мощное тело ×{curr_body/avg_body_10:.1f}")

    # 4. Shadow against direction < 30% of body
    c4 = lower_shadow / (curr_body + 1e-10) < 0.3
    conds["small_lower_shadow"] = c4
    if c4:
        met += 1; parts.append("Малая нижняя тень")

    # 5. Momentum < 0
    c5 = ind.momentum < 0
    conds["momentum_negative"] = c5
    if c5:
        met += 1; parts.append("Моментум отрицательный")

    # 6. ATR active
    c6 = ind.atr_ratio > 0.9
    conds["atr_active"] = c6
    if c6:
        met += 1; parts.append(f"ATR активный ×{ind.atr_ratio:.2f}")

    # 7. EMA pointing down
    c7a = ind.ema5 <= ind.ema13 and ind.ema13 <= ind.ema21
    c7b = ind.ema5 < float(ind.ema5_series.iloc[-3])
    conds["ema_pointing_down"] = c7a or c7b
    if c7a:
        met += 1; parts.append("EMA вниз")
    elif c7b:
        met += 1; parts.append("EMA5 поворачивает вниз")

    # Penalties (applied before base_conf calculation)
    another_sup = [s for s in levels.supports if s < broken_sup and (close[-1] - s) / close[-1] < 0.0005]
    conds["shadow_penalty"] = lower_shadow / (curr_body + 1e-10) > 0.5
    conds["wall_below"] = bool(another_sup)
    conds["atr_weak"] = ind.atr_ratio < 0.6

    if conds["shadow_penalty"]:
        met = max(0, met - 2)
    if conds["wall_below"]:
        met = max(0, met - 2)
    if conds["atr_weak"]:
        met = max(0, met - 1)

    # base_conf calculated AFTER penalties
    base_conf = met / _TOTAL * 80

    # Bonuses
    if touch_count >= 3:
        base_conf += 5
    if (broken_sup - close[-1]) / broken_sup > 0.0005:
        base_conf += 3

    # Confidence penalty multipliers (applied after base_conf)
    if conds["shadow_penalty"]:
        base_conf *= 0.6
    if conds["wall_below"]:
        base_conf *= 0.6

    return met, base_conf, parts, conds


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "micro_breakout", reason, {})
