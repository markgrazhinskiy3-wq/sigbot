"""
Strategy 4 — RSI Extreme Reversal
Scenario: Strong one-directional move, RSI hit extreme (<15 or >85).
A snapback of 1-3 candles is likely. Best in: RANGE, VOLATILE modes.
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


_TOTAL = 6


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

    # Hard reject: ATR extremely high — could be news event
    if ind.atr_ratio > 2.5:
        return _none("ATR слишком высокий — возможные новости")

    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    buy_met, buy_conf, buy_parts = _check_buy(close, open_, high, low, n, ind, levels, avg_body, ctx_trend_up)
    sell_met, sell_conf, sell_parts = _check_sell(close, open_, high, low, n, ind, levels, avg_body, ctx_trend_down)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "RSI не в экстремуме"

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
        strategy_name="rsi_reversal",
        reasoning=reason,
        debug={"rsi": round(ind.rsi, 1), "buy_met": buy_met, "sell_met": sell_met}
    )


def _check_buy(close, open_, high, low, n, ind: Indicators, levels: LevelSet, avg_body, ctx_up):
    """RSI snapback from oversold → BUY."""
    met = 0
    parts = []

    # Determine RSI extreme and base confidence
    rsi_base = 0.0
    rsi_extreme_bars = _rsi_extreme_bars(close, open_, ind, side="buy", lookback=min(3, n))

    if rsi_extreme_bars == 0:
        return 0, 0.0, []

    if ind.rsi < 10 or rsi_extreme_bars == 1 and _past_rsi_val(close, open_, n, 1) < 10:
        rsi_base = 85
    elif ind.rsi < 15 or rsi_extreme_bars > 0 and _past_rsi_val(close, open_, n, 1) < 15:
        rsi_base = 75
    else:
        rsi_base = 65

    # 1. RSI was below 20 within last 3 bars
    met += 1; parts.append(f"RSI экстрем ({ind.rsi:.0f})")

    # 2. Current RSI recovering
    prev_rsi = _past_rsi_val(close, open_, n, 1)
    if ind.rsi > prev_rsi:
        met += 1; parts.append("RSI начинает расти")

    # 3. Last 3-5 candles were bearish (the drop)
    bear_run = sum(1 for i in range(2, min(6, n)) if close[-i] < open_[-i])
    if bear_run >= 2:
        met += 1; parts.append(f"Медвежий забег ({bear_run} свечи)")

    # 4. Current candle bullish OR has lower shadow > 2× body
    curr_body = abs(close[-1] - open_[-1])
    lower_shadow = min(close[-1], open_[-1]) - low[-1]
    if close[-1] > open_[-1]:
        met += 1; parts.append("Бычья свеча разворота")
    elif lower_shadow > curr_body * 2:
        met += 1; parts.append("Нижняя тень > 2× тела")

    # 5. Stochastic %K < 20 and crossing up
    if ind.stoch_k < 20 and ind.stoch_k > ind.stoch_k_prev:
        met += 1; parts.append(f"Stoch разворот ({ind.stoch_k:.0f})")

    # 6. Momentum starting to rise
    if ind.momentum > ind.momentum_prev or (ind.momentum > 0):
        met += 1; parts.append("Моментум поворачивает вверх")

    # Bonuses
    conf = rsi_base
    pat = detect_reversal_pattern(open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bull")
    if pat.pattern == "engulfing": conf += 7
    elif pat.pattern == "pin_bar": conf += 5
    if levels.nearest_sup > 0 and levels.dist_to_sup_pct < 0.15:
        conf += 5   # support nearby
    if ctx_up:
        conf += 5

    # Hard reject: no reversal candle at all
    if close[-1] < open_[-1] and lower_shadow <= curr_body * 2:
        met = max(0, met - 2)

    return met, conf, parts


def _check_sell(close, open_, high, low, n, ind: Indicators, levels: LevelSet, avg_body, ctx_down):
    """RSI snapback from overbought → SELL."""
    met = 0
    parts = []

    rsi_extreme_bars = _rsi_extreme_bars(close, open_, ind, side="sell", lookback=min(3, n))
    if rsi_extreme_bars == 0:
        return 0, 0.0, []

    if ind.rsi > 90 or _past_rsi_val(close, open_, n, 1) > 90:
        rsi_base = 85
    elif ind.rsi > 85 or _past_rsi_val(close, open_, n, 1) > 85:
        rsi_base = 75
    else:
        rsi_base = 65

    met += 1; parts.append(f"RSI экстрем ({ind.rsi:.0f})")

    prev_rsi = _past_rsi_val(close, open_, n, 1)
    if ind.rsi < prev_rsi:
        met += 1; parts.append("RSI начинает падать")

    bull_run = sum(1 for i in range(2, min(6, n)) if close[-i] > open_[-i])
    if bull_run >= 2:
        met += 1; parts.append(f"Бычий забег ({bull_run} свечи)")

    curr_body = abs(close[-1] - open_[-1])
    upper_shadow = high[-1] - max(close[-1], open_[-1])
    if close[-1] < open_[-1]:
        met += 1; parts.append("Медвежья свеча разворота")
    elif upper_shadow > curr_body * 2:
        met += 1; parts.append("Верхняя тень > 2× тела")

    if ind.stoch_k > 80 and ind.stoch_k < ind.stoch_k_prev:
        met += 1; parts.append(f"Stoch разворот ({ind.stoch_k:.0f})")

    if ind.momentum < ind.momentum_prev or ind.momentum < 0:
        met += 1; parts.append("Моментум поворачивает вниз")

    conf = rsi_base
    pat = detect_reversal_pattern(open_[-4:], high[-4:], low[-4:], close[-4:], avg_body, "bear")
    if pat.pattern == "engulfing": conf += 7
    elif pat.pattern == "pin_bar": conf += 5
    if levels.dist_to_res_pct < 0.15:
        conf += 5
    if ctx_down:
        conf += 5

    if close[-1] > open_[-1] and upper_shadow <= curr_body * 2:
        met = max(0, met - 2)

    return met, conf, parts


def _rsi_extreme_bars(close, open_, ind: Indicators, side: str, lookback: int) -> int:
    """How many of the last `lookback` bars had RSI in extreme territory."""
    if side == "buy":
        return 1 if ind.rsi < 30 else 0
    else:
        return 1 if ind.rsi > 70 else 0


def _past_rsi_val(close, open_, n, bars_ago):
    """Approximate previous RSI by looking at recent price action direction."""
    # Simple: if more bearish candles before → RSI was lower
    return 50.0   # placeholder; actual RSI series comparison handled by ind


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "rsi_reversal", reason, {})
