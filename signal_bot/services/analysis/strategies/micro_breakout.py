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

    buy_met, buy_conf, buy_parts = _check_buy(close, open_, high, low, n, ind, levels, avg_body_10, ctx_trend_up)
    sell_met, sell_conf, sell_parts = _check_sell(close, open_, high, low, n, ind, levels, avg_body_10, ctx_trend_down)

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
        debug={"buy_met": buy_met, "sell_met": sell_met}
    )


def _check_buy(close, open_, high, low, n, ind: Indicators, levels: LevelSet, avg_body_10, ctx_up):
    """Breakout of resistance → BUY."""
    met = 0
    parts = []
    base_conf = 0.0

    # Find a resistance level with 2+ touches that current candle broke
    broken_res = None
    touch_count = 0
    for res in levels.strong_res:
        if close[-1] > res:   # closed above resistance
            broken_res = res
            touch_count = 2 if res in levels.strong_res else 1
            break

    if broken_res is None:
        return 0, 0.0, []

    met += 1; parts.append(f"Сопротивление {broken_res:.5f} найдено (2+ касания)")

    # 2. Current candle closed ABOVE resistance
    met += 1; parts.append(f"Пробой: закрылась выше {broken_res:.5f}")

    # 3. Breakout candle body > 1.5× avg body
    curr_body = abs(close[-1] - open_[-1])
    if curr_body > avg_body_10 * 1.5:
        met += 1; parts.append(f"Мощное тело ×{curr_body/avg_body_10:.1f}")

    # 4. Shadow against direction < 30% of body
    upper_shadow = high[-1] - max(close[-1], open_[-1])
    if upper_shadow / (curr_body + 1e-10) < 0.3:
        met += 1; parts.append("Малая верхняя тень")

    # 5. Momentum > 0 and rising
    if ind.momentum > 0:
        met += 1; parts.append("Моментум положительный")

    # 6. ATR rising — energy behind move
    if ind.atr_ratio > 0.9:
        met += 1; parts.append(f"ATR активный ×{ind.atr_ratio:.2f}")

    # 7. EMA(5) and EMA(13) pointing up
    if ind.ema5 >= ind.ema13 and ind.ema13 >= ind.ema21:
        met += 1; parts.append("EMA вверх")
    elif ind.ema5 > float(ind.ema5_series.iloc[-3]):
        met += 1; parts.append("EMA5 поворачивает вверх")

    base_conf = met / _TOTAL * 80

    # Bonuses
    if touch_count >= 3:                 base_conf += 5
    if (close[-1] - broken_res) / broken_res > 0.0005:
        base_conf += 3   # strong hold above level

    # Hard rejects
    if upper_shadow / (curr_body + 1e-10) > 0.5:
        met = max(0, met - 2)
        base_conf *= 0.6
    another_res = [r for r in levels.resistances if r > broken_res and (r - close[-1]) / close[-1] < 0.0005]
    if another_res:
        met = max(0, met - 2)
        base_conf *= 0.6
    if ind.atr_ratio < 0.6:
        met = max(0, met - 1)

    return met, base_conf, parts


def _check_sell(close, open_, high, low, n, ind: Indicators, levels: LevelSet, avg_body_10, ctx_down):
    """Breakout of support → SELL."""
    met = 0
    parts = []
    base_conf = 0.0

    broken_sup = None
    touch_count = 0
    for sup in levels.strong_sup:
        if close[-1] < sup:   # closed below support
            broken_sup = sup
            touch_count = 2 if sup in levels.strong_sup else 1
            break

    if broken_sup is None:
        return 0, 0.0, []

    met += 1; parts.append(f"Поддержка {broken_sup:.5f} найдена (2+ касания)")
    met += 1; parts.append(f"Пробой: закрылась ниже {broken_sup:.5f}")

    curr_body = abs(close[-1] - open_[-1])
    if curr_body > avg_body_10 * 1.5:
        met += 1; parts.append(f"Мощное тело ×{curr_body/avg_body_10:.1f}")

    lower_shadow = min(close[-1], open_[-1]) - low[-1]
    if lower_shadow / (curr_body + 1e-10) < 0.3:
        met += 1; parts.append("Малая нижняя тень")

    if ind.momentum < 0:
        met += 1; parts.append("Моментум отрицательный")

    if ind.atr_ratio > 0.9:
        met += 1; parts.append(f"ATR активный ×{ind.atr_ratio:.2f}")

    if ind.ema5 <= ind.ema13 and ind.ema13 <= ind.ema21:
        met += 1; parts.append("EMA вниз")
    elif ind.ema5 < float(ind.ema5_series.iloc[-3]):
        met += 1; parts.append("EMA5 поворачивает вниз")

    base_conf = met / _TOTAL * 80

    if touch_count >= 3:                 base_conf += 5
    if (broken_sup - close[-1]) / broken_sup > 0.0005:
        base_conf += 3

    if lower_shadow / (curr_body + 1e-10) > 0.5:
        met = max(0, met - 2); base_conf *= 0.6
    another_sup = [s for s in levels.supports if s < broken_sup and (close[-1] - s) / close[-1] < 0.0005]
    if another_sup:
        met = max(0, met - 2); base_conf *= 0.6
    if ind.atr_ratio < 0.6:
        met = max(0, met - 1)

    return met, base_conf, parts


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "micro_breakout", reason, {})
