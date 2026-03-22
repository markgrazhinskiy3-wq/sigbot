"""
Strategy 4 — RSI Extreme Reversal
Scenario: Strong one-directional move pushed RSI to extreme (<25 or >75).
Multiple confirming conditions required before signaling a snapback.
Best in: RANGE, VOLATILE modes.

8 conditions, minimum 5 required.
Confidence = (conditions_met / 8) * 85 + small bonuses (capped +12).
NO 5-min trend dependency — reversals are by definition counter-trend.
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


_TOTAL   = 8
_MIN_MET = 5


def rsi_reversal_strategy(
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

    if n < 8:
        return _none("Мало данных")

    # Hard reject: dead market — reversals need energy
    if ind.atr_ratio < 0.4:
        return _none("ATR мёртвый — рынок стоит")

    # Hard reject: RSI not extreme enough
    if 25 <= ind.rsi <= 75:
        return _none(f"RSI {ind.rsi:.0f} не в экстремуме (нужно <25 или >75)")

    avg_body  = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8
    avg_range = float(np.mean(high[-min(10, n):] - low[-min(10, n):])) or 1e-8

    if ind.rsi < 25:
        # Hard reject: price at resistance — no room to go up
        if levels.dist_to_res_pct < 0.05:
            return _none("Цена у сопротивления — нет места для роста")
        met, conf, parts = _check_buy(close, open_, high, low, n, ind, levels, avg_body, avg_range)
        direction = "BUY"
    else:
        # Hard reject: price at support — no room to go down
        if levels.dist_to_sup_pct < 0.05:
            return _none("Цена у поддержки — нет места для падения")
        met, conf, parts = _check_sell(close, open_, high, low, n, ind, levels, avg_body, avg_range)
        direction = "SELL"

    if met < _MIN_MET:
        return _none(f"Только {met}/{_TOTAL} условий — нужно минимум {_MIN_MET}")

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, conf),
        conditions_met=met,
        total_conditions=_TOTAL,
        strategy_name="rsi_reversal",
        reasoning="RSI разворот: " + " | ".join(parts),
        debug={
            "rsi":      round(ind.rsi, 1),
            "rsi_prev": round(ind.rsi_prev, 1),
            "met":      met,
            "conf":     round(conf, 1),
        },
    )


def _check_buy(close, open_, high, low, n, ind: Indicators, levels: LevelSet,
               avg_body: float, avg_range: float):
    """RSI snapback from oversold → BUY. 8 conditions, need 5."""
    met   = 0
    parts = []

    # CONDITION 1 — RSI extreme (< 25) — entry ticket
    # Guaranteed by hard reject above; always counts.
    met += 1
    parts.append(f"RSI перепродан ({ind.rsi:.0f})")

    # CONDITION 2 — RSI genuinely turning up (real previous RSI, not 50.0)
    if ind.rsi > ind.rsi_prev and ind.rsi_prev < 30:
        met += 1
        parts.append(f"RSI разворачивается вверх ({ind.rsi_prev:.0f}→{ind.rsi:.0f})")

    # CONDITION 3 — Bearish run before reversal (3+ red candles among last 5)
    bear_count = sum(1 for i in range(2, min(7, n)) if close[-i] < open_[-i])
    if bear_count >= 3:
        met += 1
        parts.append(f"Медвежий забег ({bear_count} свечей)")

    # CONDITION 4 — Reversal candle pattern (any one of three forms)
    curr_body    = abs(close[-1] - open_[-1])
    lower_shadow = min(close[-1], open_[-1]) - low[-1]
    pat = detect_reversal_pattern(open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bull")
    reversal_candle = False
    pat_label = ""

    if close[-1] > open_[-1] and curr_body > avg_body * 0.8:
        reversal_candle = True
        pat_label = f"Бычья свеча (тело ×{curr_body / avg_body:.1f})"
    elif lower_shadow > curr_body * 2.5 and lower_shadow > avg_body:
        reversal_candle = True
        pat_label = f"Пин-бар снизу (тень ×{lower_shadow / (curr_body + 1e-10):.1f})"
    elif pat.pattern == "engulfing":
        reversal_candle = True
        pat_label = "Бычье поглощение"

    if reversal_candle:
        met += 1
        parts.append(pat_label)

    # CONDITION 5 — Stochastic: both K and D oversold, K turning up
    if ind.stoch_k < 25 and ind.stoch_d < 25 and ind.stoch_k > ind.stoch_k_prev:
        met += 1
        parts.append(f"Stoch перепродан и разворачивается ({ind.stoch_k:.0f}/{ind.stoch_d:.0f})")

    # CONDITION 6 — Price near support level
    if levels.nearest_sup > 0 and levels.dist_to_sup_pct < 0.15:
        met += 1
        parts.append(f"Рядом поддержка ({levels.dist_to_sup_pct:.2f}%)")

    # CONDITION 7 — Momentum shift: was negative, now rising
    if ind.momentum > ind.momentum_prev and ind.momentum_prev < 0:
        met += 1
        parts.append(f"Моментум разворачивается ({ind.momentum_prev:+.5f}→{ind.momentum:+.5f})")

    # CONDITION 8 — Reversal candle has meaningful range (not a tiny doji)
    curr_range = high[-1] - low[-1]
    if curr_range > avg_range * 0.8:
        met += 1
        parts.append(f"Хороший размах свечи (×{curr_range / avg_range:.1f})")

    # Confidence: primarily driven by conditions_met
    base_conf = (met / _TOTAL) * 85

    # Small bonuses, capped at +12 total
    bonus = 0.0
    if ind.rsi < 15:
        bonus += 5
    elif ind.rsi < 20:
        bonus += 3
    if pat.pattern == "engulfing":
        bonus += 4
    elif pat.pattern == "pin_bar":
        bonus += 3
    if levels.nearest_sup > 0 and levels.dist_to_sup_pct < 0.05:
        bonus += 3
    bonus = min(bonus, 12.0)

    return met, base_conf + bonus, parts


def _check_sell(close, open_, high, low, n, ind: Indicators, levels: LevelSet,
                avg_body: float, avg_range: float):
    """RSI snapback from overbought → SELL. 8 conditions, need 5."""
    met   = 0
    parts = []

    # CONDITION 1 — RSI extreme (> 75) — entry ticket
    met += 1
    parts.append(f"RSI перекуплен ({ind.rsi:.0f})")

    # CONDITION 2 — RSI genuinely turning down (real previous RSI)
    if ind.rsi < ind.rsi_prev and ind.rsi_prev > 70:
        met += 1
        parts.append(f"RSI разворачивается вниз ({ind.rsi_prev:.0f}→{ind.rsi:.0f})")

    # CONDITION 3 — Bullish run before reversal (3+ green candles among last 5)
    bull_count = sum(1 for i in range(2, min(7, n)) if close[-i] > open_[-i])
    if bull_count >= 3:
        met += 1
        parts.append(f"Бычий забег ({bull_count} свечей)")

    # CONDITION 4 — Reversal candle pattern
    curr_body    = abs(close[-1] - open_[-1])
    upper_shadow = high[-1] - max(close[-1], open_[-1])
    pat = detect_reversal_pattern(open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bear")
    reversal_candle = False
    pat_label = ""

    if close[-1] < open_[-1] and curr_body > avg_body * 0.8:
        reversal_candle = True
        pat_label = f"Медвежья свеча (тело ×{curr_body / avg_body:.1f})"
    elif upper_shadow > curr_body * 2.5 and upper_shadow > avg_body:
        reversal_candle = True
        pat_label = f"Пин-бар сверху (тень ×{upper_shadow / (curr_body + 1e-10):.1f})"
    elif pat.pattern == "engulfing":
        reversal_candle = True
        pat_label = "Медвежье поглощение"

    if reversal_candle:
        met += 1
        parts.append(pat_label)

    # CONDITION 5 — Stochastic: both K and D overbought, K turning down
    if ind.stoch_k > 75 and ind.stoch_d > 75 and ind.stoch_k < ind.stoch_k_prev:
        met += 1
        parts.append(f"Stoch перекуплен и разворачивается ({ind.stoch_k:.0f}/{ind.stoch_d:.0f})")

    # CONDITION 6 — Price near resistance level
    if levels.nearest_res > 0 and levels.dist_to_res_pct < 0.15:
        met += 1
        parts.append(f"Рядом сопротивление ({levels.dist_to_res_pct:.2f}%)")

    # CONDITION 7 — Momentum shift: was positive, now falling
    if ind.momentum < ind.momentum_prev and ind.momentum_prev > 0:
        met += 1
        parts.append(f"Моментум разворачивается ({ind.momentum_prev:+.5f}→{ind.momentum:+.5f})")

    # CONDITION 8 — Reversal candle has meaningful range
    curr_range = high[-1] - low[-1]
    if curr_range > avg_range * 0.8:
        met += 1
        parts.append(f"Хороший размах свечи (×{curr_range / avg_range:.1f})")

    # Confidence: primarily driven by conditions_met
    base_conf = (met / _TOTAL) * 85

    bonus = 0.0
    if ind.rsi > 85:
        bonus += 5
    elif ind.rsi > 80:
        bonus += 3
    if pat.pattern == "engulfing":
        bonus += 4
    elif pat.pattern == "pin_bar":
        bonus += 3
    if levels.nearest_res > 0 and levels.dist_to_res_pct < 0.05:
        bonus += 3
    bonus = min(bonus, 12.0)

    return met, base_conf + bonus, parts


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "rsi_reversal", reason, {})
