"""
Strategy 1 — EMA Bounce in Micro-Trend
Scenario: Price in a trend, pulls back to EMA(13) or EMA(21), bounces continuing the trend.
Best in: TRENDING_UP / TRENDING_DOWN modes.
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


_TOTAL = 7


def ema_bounce_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
) -> StrategyResult:
    """
    Returns BUY / SELL / NONE with confidence 0-100.
    Requires >= 60% of 7 conditions met.
    """
    close = df["close"].values
    open_ = df["open"].values
    high  = df["high"].values
    low   = df["low"].values
    n     = len(df)

    if n < 8:
        return _none("Мало данных")

    price    = close[-1]
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    # ── BUY check ──────────────────────────────────────────────────────────────
    buy_score, buy_met, buy_parts = _check_buy(
        close, open_, high, low, n, price, avg_body, ind
    )
    # ── SELL check ─────────────────────────────────────────────────────────────
    sell_score, sell_met, sell_parts = _check_sell(
        close, open_, high, low, n, price, avg_body, ind
    )

    # Hard rejections
    buy_reject  = _hard_reject_buy(ind, levels, price)
    sell_reject = _hard_reject_sell(ind, levels, price)

    # Choose direction
    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Условия не выполнены"

    if buy_score > sell_score and buy_met >= 4 and not buy_reject:
        direction      = "BUY"
        conditions_met = buy_met
        base_conf      = buy_met / _TOTAL * 85
        reason         = " | ".join(buy_parts)
        # Bonuses
        if ctx_trend_up:        base_conf += 7
        ema13_tol = ind.ema13 * 1.0005
        if price <= ema13_tol and abs(low[-1] - ind.ema13) / price < 0.0005:
            base_conf += 5   # precise EMA13 touch
        if abs(close[-1] - open_[-1]) > 0 and (high[-1] - close[-1]) < abs(close[-1] - open_[-1]) * 0.2:
            base_conf += 3   # small shadow against trend
        if 45 <= ind.rsi <= 60:
            base_conf += 3

    elif sell_score > buy_score and sell_met >= 4 and not sell_reject:
        direction      = "SELL"
        conditions_met = sell_met
        base_conf      = sell_met / _TOTAL * 85
        reason         = " | ".join(sell_parts)
        if ctx_trend_down:      base_conf += 7
        ema13_tol = ind.ema13 * 0.9995
        if price >= ema13_tol and abs(high[-1] - ind.ema13) / price < 0.0005:
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
    met = 0
    parts = []
    check = min(5, n)

    # 1. EMA(5) > EMA(13) > EMA(21) for last 5 candles
    ema5_arr  = ind.ema5_series.iloc[-check:].values
    ema13_arr = ind.ema13_series.iloc[-check:].values
    ema21_arr = ind.ema21_series.iloc[-check:].values
    if int(np.sum((ema5_arr > ema13_arr) & (ema13_arr > ema21_arr))) >= 4:
        met += 1; parts.append("EMA выровнены вверх")

    # 2. Price low touched EMA(13) zone (within 0.05%)
    ema13_zone = ind.ema13 * 1.0005
    touched_ema13 = any(float(low[-i]) <= ema13_zone for i in range(1, min(4, n)))
    if touched_ema13:
        met += 1; parts.append("Коснулась EMA13")

    # 3. Candle closed ABOVE EMA(13)
    if close[-1] > ind.ema13:
        met += 1; parts.append("Закрылась выше EMA13")

    # 4. Bounce candle is bullish with body > 50% avg_body
    if close[-1] > open_[-1] and abs(close[-1] - open_[-1]) > avg_body * 0.5:
        met += 1; parts.append("Бычья свеча отскока")

    # 5. Previous 2-3 candles were pullback (small or bearish)
    if n >= 3:
        pb_candles = [close[-i] < open_[-i] or abs(close[-i] - open_[-i]) < avg_body * 0.5
                      for i in range(2, min(5, n))]
        if sum(pb_candles) >= 2:
            met += 1; parts.append("Откат перед отскоком")

    # 6. RSI(7) between 40 and 70
    if 40 <= ind.rsi <= 70:
        met += 1; parts.append(f"RSI {ind.rsi:.0f} в норме")

    # 7. Stochastic %K crossed above %D or %K > %D
    if ind.stoch_k > ind.stoch_d or (ind.stoch_k > ind.stoch_k_prev and ind.stoch_k_prev < ind.stoch_d):
        met += 1; parts.append(f"Stoch разворот вверх ({ind.stoch_k:.0f})")

    return met, met, parts


def _check_sell(close, open_, high, low, n, price, avg_body, ind: Indicators):
    met = 0
    parts = []
    check = min(5, n)

    ema5_arr  = ind.ema5_series.iloc[-check:].values
    ema13_arr = ind.ema13_series.iloc[-check:].values
    ema21_arr = ind.ema21_series.iloc[-check:].values
    if int(np.sum((ema5_arr < ema13_arr) & (ema13_arr < ema21_arr))) >= 4:
        met += 1; parts.append("EMA выровнены вниз")

    ema13_zone = ind.ema13 * 0.9995
    touched_ema13 = any(float(high[-i]) >= ema13_zone for i in range(1, min(4, n)))
    if touched_ema13:
        met += 1; parts.append("Коснулась EMA13 сверху")

    if close[-1] < ind.ema13:
        met += 1; parts.append("Закрылась ниже EMA13")

    if close[-1] < open_[-1] and abs(close[-1] - open_[-1]) > avg_body * 0.5:
        met += 1; parts.append("Медвежья свеча отскока")

    if n >= 3:
        pb_candles = [close[-i] > open_[-i] or abs(close[-i] - open_[-i]) < avg_body * 0.5
                      for i in range(2, min(5, n))]
        if sum(pb_candles) >= 2:
            met += 1; parts.append("Откат перед отскоком")

    if 30 <= ind.rsi <= 60:
        met += 1; parts.append(f"RSI {ind.rsi:.0f} в норме")

    if ind.stoch_k < ind.stoch_d or (ind.stoch_k < ind.stoch_k_prev and ind.stoch_k_prev > ind.stoch_d):
        met += 1; parts.append(f"Stoch разворот вниз ({ind.stoch_k:.0f})")

    return met, met, parts


def _hard_reject_buy(ind: Indicators, levels: LevelSet, price: float) -> bool:
    """True = do NOT generate BUY signal."""
    if ind.rsi > 78:
        return True
    if levels.dist_to_res_pct < 0.08:
        return True
    return False


def _hard_reject_sell(ind: Indicators, levels: LevelSet, price: float) -> bool:
    if ind.rsi < 22:
        return True
    if levels.dist_to_sup_pct < 0.08:
        return True
    return False


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "ema_bounce", reason, {})
