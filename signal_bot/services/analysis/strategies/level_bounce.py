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
        return _none("Мало данных", {"early_reject": "n<6"})

    # Hard reject: dead market
    if ind.atr_ratio < 0.30:
        return _none("ATR мёртвый — рынок стоит",
                     {"early_reject": f"atr_ratio={round(ind.atr_ratio,3)}<0.30",
                      "atr_ratio": round(ind.atr_ratio, 3)})

    price    = float(close[-1])
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8
    tolerance = max(0.001, avg_body / price) * price  # max(0.1%, avg_body_pct)

    best_buy, buy_conds   = _evaluate_support(close, open_, high, low, n, price, avg_body, tolerance, ind, levels)
    best_sell, sell_conds = _evaluate_resistance(close, open_, high, low, n, price, avg_body, tolerance, ind, levels)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Уровень не найден или нет паттерна"

    if best_buy[0] > best_sell[0] and best_buy[1] >= 3:
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

    elif best_sell[0] > best_buy[0] and best_sell[1] >= 3:
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
        debug={
            "buy_score": best_buy[0], "sell_score": best_sell[0],
            "buy_conditions": buy_conds,
            "sell_conditions": sell_conds,
        }
    )


def _evaluate_support(close, open_, high, low, n, price, avg_body, tolerance, ind, levels: LevelSet):
    """Returns ((base_conf, conditions_met, reasoning, touch_count), conds_dict)."""
    best = (0.0, 0, "", 0)
    best_conds: dict[str, bool] = {}

    n_sup = len(levels.supports[:5])
    best_conds["levels_available"] = n_sup > 0

    if n_sup == 0:
        best_conds["level_found"] = False
        return best, best_conds

    for sup in levels.supports[:5]:
        zone_lo = sup - tolerance
        zone_hi = sup + tolerance

        # Check touch in last 2 bars
        touched = any(float(low[-i]) <= zone_hi and float(high[-i]) >= zone_lo
                      for i in range(1, min(3, n)))
        if not touched and (price - zone_hi) / price * 100 > 0.15:
            continue

        # Get touch count from strong levels
        touch_count = 2 if sup in levels.strong_sup else 1
        if touch_count < 2:
            continue  # must have 2+ touches

        conds: dict[str, bool] = {}
        met = 0
        parts = []

        # 1. Support level with 2+ touches
        conds["level_found"] = True
        met += 1; parts.append(f"Поддержка {sup:.5f}")

        # 2. Price low within 0.2% of level in last 2 candles (relaxed from 0.1%)
        c2 = any(abs(float(low[-i]) - sup) / sup < 0.002 for i in range(1, min(3, n)))
        conds["zone_touch"] = c2
        if c2:
            met += 1; parts.append("Касание зоны")

        # 3. Reversal pattern — textbook OR simple fallback (opposite candle / wick rejection / 2 small candles)
        pat = detect_reversal_pattern(open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bull")
        fallback_rev = False
        if pat.pattern == "none":
            last_bull  = float(close[-1]) > float(open_[-1])
            tot_range  = float(high[-1]) - float(low[-1])
            lower_wick = min(float(close[-1]), float(open_[-1])) - float(low[-1])
            bull_count = sum(1 for i in range(1, min(3, n)) if float(close[-i]) > float(open_[-i]))
            if last_bull:                                                # (a) simple bullish candle
                fallback_rev = True
            elif tot_range > 0 and lower_wick / tot_range > 0.55:       # (b) long lower wick rejection
                fallback_rev = True
            elif bull_count >= 2:                                        # (c) two small bullish candles
                fallback_rev = True
        conds["reversal_pattern"] = pat.pattern != "none" or fallback_rev
        conds["pattern_type"] = pat.pattern   # type: ignore[assignment]
        if pat.pattern == "none" and not fallback_rev:
            # No reversal signal at all — skip this level
            if met > best[1]:
                best_conds = conds
            continue
        if pat.pattern == "pin_bar":
            met += 1; parts.append(f"Пин-бар (кач={pat.quality:.1f})")
        elif pat.pattern == "engulfing":
            met += 1; parts.append(f"Поглощение (кач={pat.quality:.1f})")
        elif pat.pattern == "hammer":
            met += 1; parts.append(f"Молот (кач={pat.quality:.1f})")
        elif fallback_rev:
            met += 1; parts.append("Разворотный признак ↑")

        # 4. Current candle closed ABOVE support
        c4 = float(close[-1]) > zone_lo
        conds["close_above_support"] = c4
        if c4:
            met += 1; parts.append("Закрылась выше поддержки")

        # 5. RSI < 35
        c5 = ind.rsi < 35
        conds["rsi_oversold"] = c5
        if c5:
            met += 1; parts.append(f"RSI перепродан ({ind.rsi:.0f})")

        # 6. Stoch < 25 turning up
        c6 = ind.stoch_k < 25 and ind.stoch_k > ind.stoch_k_prev
        conds["stoch_turning_up"] = c6
        if c6:
            met += 1; parts.append(f"Stoch разворот ({ind.stoch_k:.0f})")

        # 7. Room to resistance > 0.15%
        c7 = levels.dist_to_res_pct > 0.15
        conds["room_to_resistance"] = c7
        if c7:
            met += 1; parts.append(f"Пространство {levels.dist_to_res_pct:.2f}%")

        # Confidence: anchored curve (3→45, 4→53, 5→61, 6→69, 7→77) + pattern quality bonus
        conf = 45 + max(0, met - 3) * 8
        if pat.pattern == "pin_bar":     conf += pat.quality * 8
        elif pat.pattern == "engulfing": conf += pat.quality * 6
        elif pat.pattern == "hammer":    conf += pat.quality * 5

        if conf > best[0] and met >= 3:
            best = (conf, met, " | ".join(parts), touch_count)
            best_conds = conds

    return best, best_conds


def _evaluate_resistance(close, open_, high, low, n, price, avg_body, tolerance, ind, levels: LevelSet):
    """Returns ((base_conf, conditions_met, reasoning, touch_count), conds_dict)."""
    best = (0.0, 0, "", 0)
    best_conds: dict[str, bool] = {}

    n_res = len(levels.resistances[:5])
    best_conds["levels_available"] = n_res > 0

    if n_res == 0:
        best_conds["level_found"] = False
        return best, best_conds

    for res in levels.resistances[:5]:
        zone_lo = res - tolerance
        zone_hi = res + tolerance

        touched = any(float(high[-i]) >= zone_lo and float(low[-i]) <= zone_hi
                      for i in range(1, min(3, n)))
        if not touched and (zone_lo - price) / price * 100 > 0.15:
            continue

        touch_count = 2 if res in levels.strong_res else 1
        if touch_count < 2:
            continue

        conds: dict[str, bool] = {}
        met = 0
        parts = []

        conds["level_found"] = True
        met += 1; parts.append(f"Сопротивление {res:.5f}")

        # 2. Price high within 0.2% of level in last 2 candles (relaxed from 0.1%)
        c2 = any(abs(float(high[-i]) - res) / res < 0.002 for i in range(1, min(3, n)))
        conds["zone_touch"] = c2
        if c2:
            met += 1; parts.append("Касание зоны")

        pat = detect_reversal_pattern(open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bear")
        fallback_rev = False
        if pat.pattern == "none":
            last_bear  = float(close[-1]) < float(open_[-1])
            tot_range  = float(high[-1]) - float(low[-1])
            upper_wick = float(high[-1]) - max(float(close[-1]), float(open_[-1]))
            bear_count = sum(1 for i in range(1, min(3, n)) if float(close[-i]) < float(open_[-i]))
            if last_bear:                                                # (a) simple bearish candle
                fallback_rev = True
            elif tot_range > 0 and upper_wick / tot_range > 0.55:       # (b) long upper wick rejection
                fallback_rev = True
            elif bear_count >= 2:                                        # (c) two small bearish candles
                fallback_rev = True
        conds["reversal_pattern"] = pat.pattern != "none" or fallback_rev
        conds["pattern_type"] = pat.pattern   # type: ignore[assignment]
        if pat.pattern == "none" and not fallback_rev:
            if met > best[1]:
                best_conds = conds
            continue
        if pat.pattern == "pin_bar":
            met += 1; parts.append(f"Пин-бар (кач={pat.quality:.1f})")
        elif pat.pattern == "engulfing":
            met += 1; parts.append(f"Поглощение (кач={pat.quality:.1f})")
        elif pat.pattern == "hammer":
            met += 1; parts.append(f"Молот (кач={pat.quality:.1f})")
        elif fallback_rev:
            met += 1; parts.append("Разворотный признак ↓")

        c4 = float(close[-1]) < zone_hi
        conds["close_below_resistance"] = c4
        if c4:
            met += 1; parts.append("Закрылась ниже сопротивления")

        c5 = ind.rsi > 65
        conds["rsi_overbought"] = c5
        if c5:
            met += 1; parts.append(f"RSI перекуплен ({ind.rsi:.0f})")

        c6 = ind.stoch_k > 75 and ind.stoch_k < ind.stoch_k_prev
        conds["stoch_turning_down"] = c6
        if c6:
            met += 1; parts.append(f"Stoch разворот ({ind.stoch_k:.0f})")

        c7 = levels.dist_to_sup_pct > 0.15
        conds["room_to_support"] = c7
        if c7:
            met += 1; parts.append(f"Пространство {levels.dist_to_sup_pct:.2f}%")

        # Confidence: anchored curve (3→45, 4→53, 5→61, 6→69, 7→77) + pattern quality bonus
        conf = 45 + max(0, met - 3) * 8
        if pat.pattern == "pin_bar":     conf += pat.quality * 8
        elif pat.pattern == "engulfing": conf += pat.quality * 6
        elif pat.pattern == "hammer":    conf += pat.quality * 5

        if conf > best[0] and met >= 3:
            best = (conf, met, " | ".join(parts), touch_count)
            best_conds = conds

    return best, best_conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "level_bounce", reason,
                          extra or {})
