"""
Strategy: level_touch
Simplest level-bounce — no indicator confirmation.

Level detection: 1m candles, last 100 bars (~100 minutes)
Entry signal:    last closed 1m candle

SELL: resistance in 1m (2+ touches, last 100 bars)
      + last 1m HIGH >= level (±0.02%)
      + last 1m CLOSE < level
      → SELL

BUY:  support in 1m (2+ touches, last 100 bars)
      + last 1m LOW <= level (±0.02%)
      + last 1m CLOSE > level
      → BUY

Confidence: base 60
  +5  level touched 3+ times
  +5  pin bar (rejection wick)
  +3  5m trend confirms reversal direction
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from ..indicators import Indicators
from ..levels import LevelSet

import logging
logger = logging.getLogger(__name__)


@dataclass
class StrategyResult:
    direction: str
    confidence: float
    conditions_met: int
    total_conditions: int
    strategy_name: str
    reasoning: str
    debug: dict


_TOTAL     = 3        # 3 core conditions: level exists, price touched, candle closed
_TOLERANCE = 0.0002   # 0.02% proximity tolerance
_N_CANDLES = 100      # how many 1m bars to look back for level detection


def level_touch_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    df5m: pd.DataFrame | None = None,
    **kwargs,
) -> StrategyResult:
    """
    Level-touch bounce without indicator requirements.
    Uses 1m candles for level detection, last 1m candle for entry signal.
    5m data used only for trend-confirmation bonus.
    """
    if len(df) < 10:
        return _none("Мало 1m данных", {"early_reject": "n<10"})

    close = df["close"].values.astype(float)
    high  = df["high"].values.astype(float)
    low   = df["low"].values.astype(float)
    open_ = df["open"].values.astype(float)

    last_high  = high[-1]
    last_low   = low[-1]
    last_close = close[-1]
    last_open  = open_[-1]

    # Level detection on 1m, excluding the last (current) candle being evaluated
    sup_levels, res_levels = _find_1m_levels(df.iloc[:-1], n_candles=_N_CANDLES)

    trend_5m_up   = _is_5m_trend_up(df5m)
    trend_5m_down = _is_5m_trend_down(df5m)

    # ── SELL: price touched resistance, closed below ───────────────────────────
    for res_price, touch_count in res_levels:
        if touch_count < 2:
            continue

        tol = res_price * _TOLERANCE
        c1_touched = last_high >= res_price - tol   # high reached the level
        c2_closed  = last_close < res_price          # candle closed below it

        if not (c1_touched and c2_closed):
            continue

        conf  = 60.0
        parts = [f"Сопротивление {res_price:.5f} ({touch_count}x, 1m)"]

        if touch_count >= 3:
            conf += 5
            parts.append(f"Сильный уровень ({touch_count}x)")

        upper_wick  = last_high - max(last_close, last_open)
        candle_body = abs(last_close - last_open)
        if candle_body > 0 and upper_wick > candle_body * 1.5:
            conf += 5
            parts.append("Пин-бар (верхний фитиль)")

        if trend_5m_down:
            conf += 3
            parts.append("5m тренд вниз (подтверждает SELL)")

        logger.info(
            "level_touch SELL: res=%.5f touches=%d high=%.5f close=%.5f conf=%.0f",
            res_price, touch_count, last_high, last_close, conf,
        )

        return StrategyResult(
            direction="SELL",
            confidence=min(75.0, conf),
            conditions_met=3,
            total_conditions=_TOTAL,
            strategy_name="level_touch",
            reasoning=" | ".join(parts),
            debug={
                "level": round(res_price, 6),
                "touch_count": touch_count,
                "last_high": round(last_high, 6),
                "last_close": round(last_close, 6),
                "trend_5m_down": trend_5m_down,
                "n_1m_bars_used": min(len(df) - 1, _N_CANDLES),
                "sell_conditions": {
                    "level_exists":  True,
                    "high_touched":  bool(c1_touched),
                    "close_below":   bool(c2_closed),
                },
                "buy_conditions": {},
            }
        )

    # ── BUY: price touched support, closed above ───────────────────────────────
    for sup_price, touch_count in sup_levels:
        if touch_count < 2:
            continue

        tol = sup_price * _TOLERANCE
        c1_touched = last_low <= sup_price + tol    # low reached the level
        c2_closed  = last_close > sup_price          # candle closed above it

        if not (c1_touched and c2_closed):
            continue

        conf  = 60.0
        parts = [f"Поддержка {sup_price:.5f} ({touch_count}x, 1m)"]

        if touch_count >= 3:
            conf += 5
            parts.append(f"Сильный уровень ({touch_count}x)")

        lower_wick  = min(last_close, last_open) - last_low
        candle_body = abs(last_close - last_open)
        if candle_body > 0 and lower_wick > candle_body * 1.5:
            conf += 5
            parts.append("Пин-бар (нижний фитиль)")

        if trend_5m_up:
            conf += 3
            parts.append("5m тренд вверх (подтверждает BUY)")

        logger.info(
            "level_touch BUY: sup=%.5f touches=%d low=%.5f close=%.5f conf=%.0f",
            sup_price, touch_count, last_low, last_close, conf,
        )

        return StrategyResult(
            direction="BUY",
            confidence=min(75.0, conf),
            conditions_met=3,
            total_conditions=_TOTAL,
            strategy_name="level_touch",
            reasoning=" | ".join(parts),
            debug={
                "level": round(sup_price, 6),
                "touch_count": touch_count,
                "last_low": round(last_low, 6),
                "last_close": round(last_close, 6),
                "trend_5m_up": trend_5m_up,
                "n_1m_bars_used": min(len(df) - 1, _N_CANDLES),
                "sell_conditions": {},
                "buy_conditions": {
                    "level_exists":  True,
                    "low_touched":   bool(c1_touched),
                    "close_above":   bool(c2_closed),
                },
            }
        )

    return _none(
        "Нет касания уровней",
        {
            "n_res_levels": len(res_levels),
            "n_sup_levels": len(sup_levels),
            "n_1m_bars_used": min(len(df) - 1, _N_CANDLES),
            "last_high": round(last_high, 6),
            "last_low": round(last_low, 6),
            "sell_conditions": {},
            "buy_conditions": {},
        }
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_1m_levels(
    df1m: pd.DataFrame,
    n_candles: int = 100,
) -> tuple[list[tuple[float, int]], list[tuple[float, int]]]:
    """
    Find support and resistance levels from 1m candles.
    Uses local peaks (highs) for resistance and troughs (lows) for support.
    Returns (supports, resistances) as (price, touch_count) sorted by touches desc.
    """
    df = df1m.tail(n_candles)
    if len(df) < 5:
        return [], []

    highs = df["high"].values.astype(float)
    lows  = df["low"].values.astype(float)
    n     = len(highs)

    res_prices: list[float] = []
    sup_prices: list[float] = []

    for i in range(1, n - 1):
        if highs[i] >= highs[i - 1] and highs[i] >= highs[i + 1]:
            res_prices.append(highs[i])
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]:
            sup_prices.append(lows[i])

    return _cluster(sup_prices), _cluster(res_prices)


def _cluster(prices: list[float]) -> list[tuple[float, int]]:
    """Cluster nearby prices (within 0.05%) into levels with touch counts."""
    if not prices:
        return []
    prices_sorted = sorted(prices)
    clusters: list[tuple[float, int]] = []
    bucket = [prices_sorted[0]]

    for p in prices_sorted[1:]:
        ref = bucket[0]
        if ref > 0 and (p - ref) / ref < 0.0005:   # within 0.05% for 1m precision
            bucket.append(p)
        else:
            clusters.append((float(np.mean(bucket)), len(bucket)))
            bucket = [p]

    clusters.append((float(np.mean(bucket)), len(bucket)))
    return sorted(clusters, key=lambda x: x[1], reverse=True)


def _is_5m_trend_up(df5m: pd.DataFrame | None) -> bool:
    """True if 5m short EMA is above long EMA (bullish macro)."""
    if df5m is None or len(df5m) < 8:
        return False
    closes = df5m["close"].values.astype(float)
    ema5  = _ema(closes, 5)
    ema13 = _ema(closes, 13)
    return float(ema5[-1]) > float(ema13[-1])


def _is_5m_trend_down(df5m: pd.DataFrame | None) -> bool:
    """True if 5m short EMA is below long EMA (bearish macro)."""
    if df5m is None or len(df5m) < 8:
        return False
    closes = df5m["close"].values.astype(float)
    ema5  = _ema(closes, 5)
    ema13 = _ema(closes, 13)
    return float(ema5[-1]) < float(ema13[-1])


def _ema(values: np.ndarray, span: int) -> np.ndarray:
    alpha = 2.0 / (span + 1)
    result = np.empty_like(values)
    result[0] = values[0]
    for i in range(1, len(values)):
        result[i] = alpha * values[i] + (1 - alpha) * result[i - 1]
    return result


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult(
        direction="NONE",
        confidence=0.0,
        conditions_met=0,
        total_conditions=_TOTAL,
        strategy_name="level_touch",
        reasoning=reason,
        debug=extra or {},
    )
