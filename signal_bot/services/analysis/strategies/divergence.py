"""
Strategy 6 — Micro Divergence
Scenario: Price makes new high/low but RSI does not confirm → exhaustion → snapback.
Best in: RANGE, VOLATILE modes.
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


_TOTAL = 5


def divergence_strategy(
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

    if n < 15:
        return _none("Мало данных для дивергенции")

    # Divergence works in moderate conditions — lower bar than breakouts
    if ind.atr_ratio < 0.35:
        return _none("ATR мёртвый — рынок стоит")

    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    buy_met, buy_conf, buy_parts = _check_bullish_div(
        close, open_, high, low, n, ind, levels, avg_body, ctx_trend_up, mode
    )
    sell_met, sell_conf, sell_parts = _check_bearish_div(
        close, open_, high, low, n, ind, levels, avg_body, ctx_trend_down, mode
    )

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Дивергенция не обнаружена"

    if buy_conf > sell_conf and buy_met >= 3:
        direction = "BUY"; conditions_met = buy_met; base_conf = buy_conf
        reason = " | ".join(buy_parts)
    elif sell_conf > buy_conf and sell_met >= 3:
        direction = "SELL"; conditions_met = sell_met; base_conf = sell_conf
        reason = " | ".join(sell_parts)

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="divergence",
        reasoning=reason,
        debug={"buy_met": buy_met, "sell_met": sell_met, "rsi": round(ind.rsi, 1)}
    )


def _find_two_lows(low: np.ndarray, n: int, lookback: int = 20) -> tuple[int, int] | None:
    """Find two local minima within lookback bars. Returns (idx1, idx2) with idx2 > idx1."""
    scan = low[-lookback:]
    m = len(scan)
    lows_idx = []
    for i in range(1, m - 1):
        if scan[i] <= scan[i-1] and scan[i] <= scan[i+1]:
            lows_idx.append(i)
    if len(lows_idx) < 2:
        return None
    # Take last two
    i1, i2 = lows_idx[-2], lows_idx[-1]
    if scan[i2] < scan[i1]:   # second low is lower — needed for divergence
        return (i1, i2)
    return None


def _find_two_highs(high: np.ndarray, n: int, lookback: int = 20) -> tuple[int, int] | None:
    """Find two local maxima. Returns (idx1, idx2) with idx2 > idx1."""
    scan = high[-lookback:]
    m = len(scan)
    highs_idx = []
    for i in range(1, m - 1):
        if scan[i] >= scan[i-1] and scan[i] >= scan[i+1]:
            highs_idx.append(i)
    if len(highs_idx) < 2:
        return None
    i1, i2 = highs_idx[-2], highs_idx[-1]
    if scan[i2] > scan[i1]:   # second high is higher
        return (i1, i2)
    return None


def _approx_rsi_at(close: np.ndarray, idx: int, lookback: int, period: int = 7) -> float:
    """Approximate RSI at a given index using a small window."""
    start = max(0, idx - period * 2)
    sub = pd.Series(close[start:idx+1])
    delta = sub.diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    ag    = gain.ewm(com=period - 1, min_periods=period).mean()
    al    = loss.ewm(com=period - 1, min_periods=period).mean()
    rs    = ag / al.replace(0, np.nan)
    r     = 100 - (100 / (1 + rs))
    v     = float(r.iloc[-1])
    return v if not np.isnan(v) else 50.0


def _check_bullish_div(close, open_, high, low, n, ind, levels, avg_body, ctx_up, mode):
    """Bullish divergence: price makes lower low, RSI makes higher low → BUY."""
    lookback = min(20, n - 1)
    pair = _find_two_lows(low, n, lookback)
    if pair is None:
        return 0, 0.0, []

    i1, i2 = pair
    abs_i1 = n - lookback + i1
    abs_i2 = n - lookback + i2

    low1 = low[abs_i1]
    low2 = low[abs_i2]
    if low2 >= low1:   # must be lower
        return 0, 0.0, []

    rsi1 = _approx_rsi_at(close, abs_i1, lookback)
    rsi2 = _approx_rsi_at(close, abs_i2, lookback)

    rsi_diff = rsi2 - rsi1
    if rsi_diff < 5:   # divergence must be meaningful (RSI not confirming the new low)
        return 0, 0.0, []
    if rsi2 >= rsi1:
        pass  # higher RSI at lower price = bullish divergence — correct!
    else:
        return 0, 0.0, []

    met = 0
    parts = []

    # 1. Two price lows, second lower
    met += 1; parts.append(f"Бычья дивергенция: цена {low2:.5f} < {low1:.5f}")

    # 2. RSI at second low > RSI at first low
    met += 1; parts.append(f"RSI: {rsi2:.0f} > {rsi1:.0f} (рост при падении цены)")

    # 3. RSI difference > 5
    if rsi_diff > 5:
        met += 1; parts.append(f"Разница RSI {rsi_diff:.0f}pts — значимая")

    # 4. Bullish candle appeared after second low
    if n > abs_i2 and close[-1] > open_[-1]:
        met += 1; parts.append("Бычья свеча после минимума")
    elif n > abs_i2:
        lower_shadow = min(close[-1], open_[-1]) - low[-1]
        if lower_shadow > abs(close[-1] - open_[-1]) * 1.5:
            met += 1; parts.append("Нижняя тень (бычье давление)")

    # 5. Stochastic turning up
    if ind.stoch_k > ind.stoch_k_prev:
        met += 1; parts.append(f"Stoch поворачивает вверх ({ind.stoch_k:.0f})")

    # Confidence: conditions-driven (not hardcoded)
    base_conf = (met / _TOTAL) * 85
    if _divergence_at_level(low2, levels.supports): base_conf += 10
    if abs(rsi_diff) > 10:                          base_conf += 7  # strong divergence
    if mode == "RANGE":                             base_conf += 5

    return met, base_conf, parts


def _check_bearish_div(close, open_, high, low, n, ind, levels, avg_body, ctx_down, mode):
    """Bearish divergence: price makes higher high, RSI makes lower high → SELL."""
    lookback = min(20, n - 1)
    pair = _find_two_highs(high, n, lookback)
    if pair is None:
        return 0, 0.0, []

    i1, i2 = pair
    abs_i1 = n - lookback + i1
    abs_i2 = n - lookback + i2

    high1 = high[abs_i1]
    high2 = high[abs_i2]
    if high2 <= high1:
        return 0, 0.0, []

    rsi1 = _approx_rsi_at(close, abs_i1, lookback)
    rsi2 = _approx_rsi_at(close, abs_i2, lookback)

    rsi_diff = rsi1 - rsi2   # rsi2 < rsi1 at higher price = bearish divergence
    if rsi_diff < 5:
        return 0, 0.0, []
    if rsi2 >= rsi1:
        return 0, 0.0, []

    met = 0
    parts = []

    met += 1; parts.append(f"Медвежья дивергенция: цена {high2:.5f} > {high1:.5f}")
    met += 1; parts.append(f"RSI: {rsi2:.0f} < {rsi1:.0f} (падение при росте цены)")

    if rsi_diff > 5:
        met += 1; parts.append(f"Разница RSI {rsi_diff:.0f}pts — значимая")

    if n > abs_i2 and close[-1] < open_[-1]:
        met += 1; parts.append("Медвежья свеча после максимума")
    elif n > abs_i2:
        upper_shadow = high[-1] - max(close[-1], open_[-1])
        if upper_shadow > abs(close[-1] - open_[-1]) * 1.5:
            met += 1; parts.append("Верхняя тень (медвежье давление)")

    if ind.stoch_k < ind.stoch_k_prev:
        met += 1; parts.append(f"Stoch поворачивает вниз ({ind.stoch_k:.0f})")

    # Confidence: conditions-driven (not hardcoded)
    base_conf = (met / _TOTAL) * 85
    if _divergence_at_level(high2, levels.resistances): base_conf += 10
    if rsi_diff > 10:                                   base_conf += 7
    if mode == "RANGE":                                 base_conf += 5

    return met, base_conf, parts


def _divergence_at_level(price: float, levels: list[float]) -> bool:
    """Is divergence extreme close to a known S/R level?"""
    return any(abs(price - lvl) / max(price, 1e-10) < 0.002 for lvl in levels)


def _none(reason: str) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "divergence", reason, {})
