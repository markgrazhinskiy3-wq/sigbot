"""
Strategy 3 — Level Bounce
Scenario: Price reaches a significant S/R level (2+ touches) and forms a reversal pattern.
Best in: RANGE mode.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet
from ..candle_patterns import detect_reversal_pattern


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


def level_bounce_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
) -> StrategyResult:
    close = df["close"].values
    open_ = df["open"].values
    high  = df["high"].values
    low   = df["low"].values
    n     = len(df)

    if n < 6:
        return _none("Мало данных")

    # Hard reject: dead market
    # Level bounces work even in calmer markets — lower ATR bar than breakouts
    if ind.atr_ratio < 0.30:
        return _none("ATR мёртвый — рынок стоит")

    price    = float(close[-1])
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8
    tolerance = max(0.001, avg_body / price) * price  # max(0.1%, avg_body_pct)

    best_buy  = _evaluate_support(close, open_, high, low, n, price, avg_body, tolerance, ind, levels)
    best_sell = _evaluate_resistance(close, open_, high, low, n, price, avg_body, tolerance, ind, levels)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Уровень не найден или нет паттерна"

    if best_buy[0] > best_sell[0] and best_buy[1] >= 4:
        direction      = "BUY"
        conditions_met = best_buy[1]
        base_conf      = best_buy[0]
        reason         = best_buy[2]
        if levels.dist_to_res_pct > 0.15:  base_conf += 7   # level confirmed on 5-min
        if best_buy[3] >= 3:               base_conf += 5   # 3+ touches
        if mode == "RANGE":                base_conf += 5
        if ind.rsi < 35:                   base_conf += 3
        if ind.stoch_k < 25 and ind.stoch_k > ind.stoch_k_prev:
            base_conf += 3

    elif best_sell[0] > best_buy[0] and best_sell[1] >= 4:
        direction      = "SELL"
        conditions_met = best_sell[1]
        base_conf      = best_sell[0]
        reason         = best_sell[2]
        if levels.dist_to_sup_pct > 0.15:  base_conf += 7
        if best_sell[3] >= 3:              base_conf += 5
        if mode == "RANGE":                base_conf += 5
        if ind.rsi > 65:                   base_conf += 3
        if ind.stoch_k > 75 and ind.stoch_k < ind.stoch_k_prev:
            base_conf += 3

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="level_bounce",
        reasoning=reason,
        debug={"buy_score": best_buy[0], "sell_score": best_sell[0]}
    )


def _evaluate_support(close, open_, high, low, n, price, avg_body, tolerance, ind, levels: LevelSet):
    """Returns (base_conf, conditions_met, reasoning, touch_count)."""
    best = (0.0, 0, "", 0)

    for sup in levels.supports[:5]:
        zone_lo = sup - tolerance
        zone_hi = sup + tolerance

        # Check touch in last 2 bars
        touched = any(float(low[-i]) <= zone_hi and float(high[-i]) >= zone_lo
                      for i in range(1, min(3, n)))
        if not touched and (price - zone_hi) / price * 100 > 0.1:
            continue

        # Get touch count from strong levels
        touch_count = 2 if sup in levels.strong_sup else 1
        if touch_count < 2:
            continue  # must have 2+ touches

        met = 0
        parts = []

        # 1. Support level with 2+ touches
        met += 1; parts.append(f"Поддержка {sup:.5f}")

        # 2. Price low within 0.1% of level in last 2 candles
        if any(abs(float(low[-i]) - sup) / sup < 0.001 for i in range(1, min(3, n))):
            met += 1; parts.append("Касание зоны")

        # 3. Reversal pattern — required, no pattern → skip this level
        pat = detect_reversal_pattern(open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bull")
        if pat.pattern == "none":
            continue
        if pat.pattern == "pin_bar":
            met += 1; parts.append(f"Пин-бар (кач={pat.quality:.1f})")
        elif pat.pattern == "engulfing":
            met += 1; parts.append(f"Поглощение (кач={pat.quality:.1f})")
        elif pat.pattern == "hammer":
            met += 1; parts.append(f"Молот (кач={pat.quality:.1f})")

        # 4. Current candle closed ABOVE support
        if float(close[-1]) > zone_lo:
            met += 1; parts.append("Закрылась выше поддержки")

        # 5. RSI < 35
        if ind.rsi < 35:
            met += 1; parts.append(f"RSI перепродан ({ind.rsi:.0f})")

        # 6. Stoch < 25 turning up
        if ind.stoch_k < 25 and ind.stoch_k > ind.stoch_k_prev:
            met += 1; parts.append(f"Stoch разворот ({ind.stoch_k:.0f})")

        # 7. Room to resistance > 0.15%
        if levels.dist_to_res_pct > 0.15:
            met += 1; parts.append(f"Пространство {levels.dist_to_res_pct:.2f}%")

        # Confidence: conditions-driven + pattern quality bonus
        conf = (met / _TOTAL) * 85
        if pat.pattern == "pin_bar":     conf += pat.quality * 8
        elif pat.pattern == "engulfing": conf += pat.quality * 6
        elif pat.pattern == "hammer":    conf += pat.quality * 5

        if conf > best[0] and met >= 4:
            best = (conf, met, " | ".join(parts), touch_count)

    return best


def _evaluate_resistance(close, open_, high, low, n, price, avg_body, tolerance, ind, levels: LevelSet):
    """Returns (base_conf, conditions_met, reasoning, touch_count)."""
    best = (0.0, 0, "", 0)

    for res in levels.resistances[:5]:
        zone_lo = res - tolerance
        zone_hi = res + tolerance

        touched = any(float(high[-i]) >= zone_lo and float(low[-i]) <= zone_hi
                      for i in range(1, min(3, n)))
        if not touched and (zone_lo - price) / price * 100 > 0.1:
            continue

        touch_count = 2 if res in levels.strong_res else 1
        if touch_count < 2:
            continue

        met = 0
        parts = []

        met += 1; parts.append(f"Сопротивление {res:.5f}")

        if any(abs(float(high[-i]) - res) / res < 0.001 for i in range(1, min(3, n))):
            met += 1; parts.append("Касание зоны")

        pat = detect_reversal_pattern(open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bear")
        if pat.pattern == "none":
            continue
        if pat.pattern == "pin_bar":
            met += 1; parts.append(f"Пин-бар (кач={pat.quality:.1f})")
        elif pat.pattern == "engulfing":
            met += 1; parts.append(f"Поглощение (кач={pat.quality:.1f})")
        elif pat.pattern == "hammer":
            met += 1; parts.append(f"Молот (кач={pat.quality:.1f})")

        if float(close[-1]) < zone_hi:
            met += 1; parts.append("Закрылась ниже сопротивления")

        if ind.rsi > 65:
            met += 1; parts.append(f"RSI перекуплен ({ind.rsi:.0f})")

        if ind.stoch_k > 75 and ind.stoch_k < ind.stoch_k_prev:
            met += 1; parts.append(f"Stoch разворот ({ind.stoch_k:.0f})")

        if levels.dist_to_sup_pct > 0.15:
            met += 1; parts.append(f"Пространство {levels.dist_to_sup_pct:.2f}%")

        # Confidence: conditions-driven + pattern quality bonus
        conf = (met / _TOTAL) * 85
        if pat.pattern == "pin_bar":     conf += pat.quality * 8
        elif pat.pattern == "engulfing": conf += pat.quality * 6
        elif pat.pattern == "hammer":    conf += pat.quality * 5

        if conf > best[0] and met >= 4:
            best = (conf, met, " | ".join(parts), touch_count)

    return best


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "level_bounce", reason, {})
