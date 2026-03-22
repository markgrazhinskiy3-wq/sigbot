"""
Strategy 1 — EMA Bounce in Micro-Trend
Scenario: Price in a trend, pulls back to EMA(13), bounces continuing the trend.
Best in: TRENDING_UP / TRENDING_DOWN modes.

8 conditions, minimum 5 required (tighter than before).
Touch zone tightened to ±0.02%. Pullback requires 3/4 red candles + one below EMA13.
Condition 8 added: bounce candle must close with conviction.
No 5-min trend bonus — short expiry doesn't need higher-TF confirmation.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet


@dataclass
class StrategyResult:
    direction: str          # "BUY" | "SELL" | "NONE"
    confidence: float       # 0-100
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL   = 8
_MIN_MET = 5


def ema_bounce_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
) -> StrategyResult:
    """
    Returns BUY / SELL / NONE with confidence 0-100.
    Requires >= 5 of 8 conditions met.
    """
    close = df["close"].values
    open_ = df["open"].values
    high  = df["high"].values
    low   = df["low"].values
    n     = len(df)

    if n < 8:
        return _none("Мало данных")

    # Hard reject: dead market
    if ind.atr_ratio < 0.4:
        return _none("ATR мёртвый — рынок стоит")

    price    = close[-1]
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    buy_score, buy_met, buy_parts   = _check_buy(close, open_, high, low, n, price, avg_body, ind)
    sell_score, sell_met, sell_parts = _check_sell(close, open_, high, low, n, price, avg_body, ind)

    buy_reject  = _hard_reject_buy(ind, levels, price)
    sell_reject = _hard_reject_sell(ind, levels, price)

    direction      = "NONE"
    conditions_met = 0
    base_conf      = 0.0
    reason         = "Условия не выполнены"

    if buy_score > sell_score and buy_met >= _MIN_MET and not buy_reject:
        direction      = "BUY"
        conditions_met = buy_met
        base_conf      = buy_met / _TOTAL * 85
        reason         = " | ".join(buy_parts)
        # Precision touch bonus (very close to EMA13 — within 0.01%)
        if abs(low[-1] - ind.ema13) / price < 0.0001:
            base_conf += 5
        # Small candle shadow (conviction in direction)
        if abs(close[-1] - open_[-1]) > 0 and (high[-1] - close[-1]) < abs(close[-1] - open_[-1]) * 0.2:
            base_conf += 3
        # RSI in comfortable buy zone
        if 45 <= ind.rsi <= 60:
            base_conf += 3

    elif sell_score > buy_score and sell_met >= _MIN_MET and not sell_reject:
        direction      = "SELL"
        conditions_met = sell_met
        base_conf      = sell_met / _TOTAL * 85
        reason         = " | ".join(sell_parts)
        if abs(high[-1] - ind.ema13) / price < 0.0001:
            base_conf += 5
        if abs(close[-1] - open_[-1]) > 0 and (close[-1] - low[-1]) < abs(close[-1] - open_[-1]) * 0.2:
            base_conf += 3
        if 40 <= ind.rsi <= 55:
            base_conf += 3

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="ema_bounce",
        reasoning=reason,
        debug={"buy_met": buy_met, "sell_met": sell_met,
               "buy_reject": buy_reject, "sell_reject": sell_reject}
    )


def _check_buy(close, open_, high, low, n, price, avg_body, ind: Indicators):
    met   = 0
    parts = []
    check = min(5, n)

    # 1. EMA(5) > EMA(13) > EMA(21) for last 5 candles — uptrend alignment
    ema5_arr  = ind.ema5_series.iloc[-check:].values
    ema13_arr = ind.ema13_series.iloc[-check:].values
    ema21_arr = ind.ema21_series.iloc[-check:].values
    if int(np.sum((ema5_arr > ema13_arr) & (ema13_arr > ema21_arr))) >= 4:
        met += 1; parts.append("EMA выровнены вверх")

    # 2. Price low touched EMA(13) zone — tightened to ±0.02% (was ±0.05%)
    ema13_zone = ind.ema13 * 1.0002
    touched_ema13 = any(float(low[-i]) <= ema13_zone for i in range(1, min(4, n)))
    if touched_ema13:
        met += 1; parts.append("Коснулась EMA13 (±0.02%)")

    # 3. Candle closed ABOVE EMA(13)
    if close[-1] > ind.ema13:
        met += 1; parts.append("Закрылась выше EMA13")

    # 4. Bounce candle is bullish with body > 50% avg_body
    if close[-1] > open_[-1] and abs(close[-1] - open_[-1]) > avg_body * 0.5:
        met += 1; parts.append("Бычья свеча отскока")

    # 5. Real pullback: 3 of 4 previous candles bearish AND at least one closed below EMA13
    #    (tightened from: 2 of 3 candles bearish)
    if n >= 5:
        pb_window = range(2, min(6, n))
        pb_bearish = [close[-i] < open_[-i] or abs(close[-i] - open_[-i]) < avg_body * 0.5
                      for i in pb_window]
        pb_below_ema = any(float(close[-i]) < ind.ema13 for i in pb_window)
        if sum(pb_bearish) >= 3 and pb_below_ema:
            met += 1; parts.append("Откат к EMA13 (3/4 свечи медвежьи, одна под EMA)")

    # 6. RSI(7) between 40 and 70 (not overheated, not oversold)
    if 40 <= ind.rsi <= 70:
        met += 1; parts.append(f"RSI {ind.rsi:.0f} в норме")

    # 7. Stochastic %K crossed above %D or %K > %D
    if ind.stoch_k > ind.stoch_d or (ind.stoch_k > ind.stoch_k_prev and ind.stoch_k_prev < ind.stoch_d):
        met += 1; parts.append(f"Stoch разворот вверх ({ind.stoch_k:.0f})")

    # 8. NEW — Bounce candle shows conviction: body > 0.7× avg AND closes in upper 60% of range
    total_range = high[-1] - low[-1]
    if (abs(close[-1] - open_[-1]) > avg_body * 0.7 and
            total_range > 0 and (close[-1] - low[-1]) / total_range > 0.6):
        met += 1; parts.append("Свеча закрылась в верхней зоне диапазона")

    return met, met, parts


def _check_sell(close, open_, high, low, n, price, avg_body, ind: Indicators):
    met   = 0
    parts = []
    check = min(5, n)

    # 1. EMA(5) < EMA(13) < EMA(21) — downtrend alignment
    ema5_arr  = ind.ema5_series.iloc[-check:].values
    ema13_arr = ind.ema13_series.iloc[-check:].values
    ema21_arr = ind.ema21_series.iloc[-check:].values
    if int(np.sum((ema5_arr < ema13_arr) & (ema13_arr < ema21_arr))) >= 4:
        met += 1; parts.append("EMA выровнены вниз")

    # 2. Price high touched EMA(13) zone — tightened to ±0.02%
    ema13_zone = ind.ema13 * 0.9998
    touched_ema13 = any(float(high[-i]) >= ema13_zone for i in range(1, min(4, n)))
    if touched_ema13:
        met += 1; parts.append("Коснулась EMA13 сверху (±0.02%)")

    # 3. Candle closed BELOW EMA(13)
    if close[-1] < ind.ema13:
        met += 1; parts.append("Закрылась ниже EMA13")

    # 4. Bounce candle is bearish with body > 50% avg_body
    if close[-1] < open_[-1] and abs(close[-1] - open_[-1]) > avg_body * 0.5:
        met += 1; parts.append("Медвежья свеча отскока")

    # 5. Real pullback: 3 of 4 previous candles bullish AND at least one closed above EMA13
    if n >= 5:
        pb_window = range(2, min(6, n))
        pb_bullish = [close[-i] > open_[-i] or abs(close[-i] - open_[-i]) < avg_body * 0.5
                      for i in pb_window]
        pb_above_ema = any(float(close[-i]) > ind.ema13 for i in pb_window)
        if sum(pb_bullish) >= 3 and pb_above_ema:
            met += 1; parts.append("Откат к EMA13 (3/4 свечи бычьи, одна над EMA)")

    # 6. RSI(7) between 30 and 60
    if 30 <= ind.rsi <= 60:
        met += 1; parts.append(f"RSI {ind.rsi:.0f} в норме")

    # 7. Stochastic %K crossed below %D or %K < %D
    if ind.stoch_k < ind.stoch_d or (ind.stoch_k < ind.stoch_k_prev and ind.stoch_k_prev > ind.stoch_d):
        met += 1; parts.append(f"Stoch разворот вниз ({ind.stoch_k:.0f})")

    # 8. NEW — Bounce candle shows conviction: body > 0.7× avg AND closes in lower 60% of range
    total_range = high[-1] - low[-1]
    if (abs(close[-1] - open_[-1]) > avg_body * 0.7 and
            total_range > 0 and (high[-1] - close[-1]) / total_range > 0.6):
        met += 1; parts.append("Свеча закрылась в нижней зоне диапазона")

    return met, met, parts


def _hard_reject_buy(ind: Indicators, levels: LevelSet, price: float) -> bool:
    """True = do NOT generate BUY signal."""
    if ind.rsi > 78:
        return True
    # Tightened: was 0.05%, now 0.08% — more room required before resistance
    if levels.dist_to_res_pct < 0.08:
        return True
    return False


def _hard_reject_sell(ind: Indicators, levels: LevelSet, price: float) -> bool:
    """True = do NOT generate SELL signal."""
    if ind.rsi < 22:
        return True
    if levels.dist_to_sup_pct < 0.08:
        return True
    return False


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "ema_bounce", reason, {})
