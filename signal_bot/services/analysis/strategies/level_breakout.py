"""
Strategy 7 — Level Breakout (1m levels + 15s entry)

STEP 1: Find levels on 1m candles (need 3+ touches for breakout validity).
STEP 2: Detect breakout close on 15s candles + momentum confirmation.

Need 5 of 6 conditions. Base confidence: 40 + (met-4)*10.
All 6 conditions are always evaluated and returned in debug.
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


_TOTAL   = 6
_MIN_MET = 5


def find_1m_levels(df1m: pd.DataFrame) -> tuple[list[tuple[float, int]], list[tuple[float, int]]]:
    """
    Scan last 50 1m candles for S/R levels.
    Returns (supports, resistances) as list of (price, touch_count) tuples,
    sorted by touch_count descending.
    """
    _CLUSTER_PCT = 0.0012
    n = min(50, len(df1m))
    highs = df1m["high"].values[-n:].astype(float)
    lows  = df1m["low"].values[-n:].astype(float)

    res_prices: list[float] = []
    for i in range(1, n - 1):
        if highs[i] >= highs[i - 1] and highs[i] >= highs[i + 1]:
            res_prices.append(highs[i])

    sup_prices: list[float] = []
    for i in range(1, n - 1):
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]:
            sup_prices.append(lows[i])

    def _cluster(prices: list[float]) -> list[tuple[float, int]]:
        if not prices:
            return []
        prices_s = sorted(prices)
        used = [False] * len(prices_s)
        out: list[tuple[float, int]] = []
        for i, p in enumerate(prices_s):
            if used[i]:
                continue
            group = [p]
            used[i] = True
            for j in range(i + 1, len(prices_s)):
                if not used[j] and abs(prices_s[j] - p) / p < _CLUSTER_PCT:
                    group.append(prices_s[j])
                    used[j] = True
            out.append((float(np.mean(group)), len(group)))
        out.sort(key=lambda x: x[1], reverse=True)
        return out

    return _cluster(sup_prices), _cluster(res_prices)


def level_breakout_strategy(
    df: pd.DataFrame,                       # 15s candles (entry timing)
    ind: Indicators,
    levels: LevelSet,                       # kept for API compatibility
    df1m_ctx: pd.DataFrame | None = None,   # 1m candles (level detection)
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
) -> StrategyResult:
    if df1m_ctx is None or len(df1m_ctx) < 10:
        return _none("Нет 1m данных", {"early_reject": "no_1m_data"})

    n = len(df)
    if n < 6:
        return _none("Мало 15s данных", {"early_reject": "n<6"})

    if ind.atr_ratio < 0.40:
        return _none("ATR слишком мал для пробоя",
                     {"early_reject": f"atr_ratio={round(ind.atr_ratio,3)}<0.40"})

    close    = df["close"].values
    open_    = df["open"].values
    high     = df["high"].values
    low      = df["low"].values
    price    = float(close[-1])
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    sup_levels, res_levels = find_1m_levels(df1m_ctx)

    best_buy  = _eval_breakout_buy(close, open_, high, low, n, price, avg_body,
                                   ind, res_levels, mode)
    best_sell = _eval_breakout_sell(close, open_, high, low, n, price, avg_body,
                                    ind, sup_levels, mode)

    direction      = "NONE"
    conditions_met = 0
    base_conf      = 0.0
    reason         = "Нет пробоя уровня"

    if best_buy["met"] >= _MIN_MET and best_buy["conf"] > best_sell["conf"]:
        direction      = "BUY"
        conditions_met = best_buy["met"]
        base_conf      = best_buy["conf"]
        reason         = best_buy["reason"]
    elif best_sell["met"] >= _MIN_MET and best_sell["conf"] > best_buy["conf"]:
        direction      = "SELL"
        conditions_met = best_sell["met"]
        base_conf      = best_sell["conf"]
        reason         = best_sell["reason"]

    return StrategyResult(
        direction=direction,
        confidence=min(100.0, base_conf),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="level_breakout",
        reasoning=reason,
        debug={
            "buy_score":       best_buy["conf"],
            "sell_score":      best_sell["conf"],
            "buy_conditions":  best_buy["conds"],
            "sell_conditions": best_sell["conds"],
            "buy_met":         best_buy["met"],
            "sell_met":        best_sell["met"],
            "sup_levels_1m":   [(round(p, 5), t) for p, t in sup_levels[:5]],
            "res_levels_1m":   [(round(p, 5), t) for p, t in res_levels[:5]],
        }
    )


# ── BUY: breakout above 1m resistance ────────────────────────────────────────

def _eval_breakout_buy(close, open_, high, low, n, price, avg_body,
                       ind, res_levels, mode) -> dict:
    """
    Evaluate all 6 conditions for every resistance level with 3+ touches.
    Always returns best partial result for debug visibility.
    """
    best: dict = {"met": 0, "conf": 0.0, "reason": "", "conds": {}}

    for res_price, touch_count in res_levels[:5]:
        if touch_count < 3:
            continue

        conds: dict[str, bool] = {}
        met   = 0
        parts: list[str] = []

        # 1. tested_level: 3+ touches on 1m chart
        c1 = True
        conds["tested_level"] = c1
        met += 1
        parts.append(f"Протестир. сопр. {res_price:.5f} ({touch_count}x)")

        # 2. close_beyond: 15s close ABOVE resistance
        c2 = float(close[-1]) > res_price
        conds["close_beyond"] = c2
        if c2:
            met += 1
            parts.append(f"Закрылась выше {res_price:.5f}")

        # 3. momentum_candle: bullish body > 1.0x avg (was 1.5x — too rare on 15s)
        body = abs(float(close[-1]) - float(open_[-1]))
        c3 = body > avg_body * 1.0 and float(close[-1]) > float(open_[-1])
        conds["momentum_candle"] = c3
        if c3:
            met += 1
            parts.append(f"Импульс (тело {body/avg_body:.1f}x avg)")

        # 4. follow_through: previous 15s candle also bullish
        c4 = n >= 2 and float(close[-2]) > float(open_[-2])
        conds["follow_through"] = c4
        if c4:
            met += 1
            parts.append("Продолжение (пред. свеча бычья)")

        # 5. ema_aligned: EMA5 > EMA13
        c5 = ind.ema5 > ind.ema13
        conds["ema_aligned"] = c5
        if c5:
            met += 1
            parts.append("EMA вверх")

        # 6. not_exhausted: RSI 35-65
        c6 = 35 <= ind.rsi <= 65
        conds["not_exhausted"] = c6
        if c6:
            met += 1
            parts.append(f"RSI в норме ({ind.rsi:.0f})")

        conf = 0.0
        if met >= _MIN_MET:
            conf = 40 + max(0, met - 4) * 10
            if touch_count >= 4: conf += 5

        if met > best["met"] or (met >= _MIN_MET and conf > best["conf"]):
            best = {"met": met, "conf": conf, "reason": " | ".join(parts), "conds": conds}

    return best


# ── SELL: breakout below 1m support ──────────────────────────────────────────

def _eval_breakout_sell(close, open_, high, low, n, price, avg_body,
                        ind, sup_levels, mode) -> dict:
    """
    Evaluate all 6 conditions for every support level with 3+ touches.
    Always returns best partial result for debug visibility.
    """
    best: dict = {"met": 0, "conf": 0.0, "reason": "", "conds": {}}

    for sup_price, touch_count in sup_levels[:5]:
        if touch_count < 3:
            continue

        conds: dict[str, bool] = {}
        met   = 0
        parts: list[str] = []

        # 1. tested_level: 3+ touches
        c1 = True
        conds["tested_level"] = c1
        met += 1
        parts.append(f"Протестир. пд. {sup_price:.5f} ({touch_count}x)")

        # 2. close_beyond: 15s close BELOW support
        c2 = float(close[-1]) < sup_price
        conds["close_beyond"] = c2
        if c2:
            met += 1
            parts.append(f"Закрылась ниже {sup_price:.5f}")

        # 3. momentum_candle: bearish body > 1.0x avg (was 1.5x — too rare on 15s)
        body = abs(float(close[-1]) - float(open_[-1]))
        c3 = body > avg_body * 1.0 and float(close[-1]) < float(open_[-1])
        conds["momentum_candle"] = c3
        if c3:
            met += 1
            parts.append(f"Импульс (тело {body/avg_body:.1f}x avg)")

        # 4. follow_through: previous 15s candle also bearish
        c4 = n >= 2 and float(close[-2]) < float(open_[-2])
        conds["follow_through"] = c4
        if c4:
            met += 1
            parts.append("Продолжение (пред. свеча медвежья)")

        # 5. ema_aligned: EMA5 < EMA13
        c5 = ind.ema5 < ind.ema13
        conds["ema_aligned"] = c5
        if c5:
            met += 1
            parts.append("EMA вниз")

        # 6. not_exhausted: RSI 35-65
        c6 = 35 <= ind.rsi <= 65
        conds["not_exhausted"] = c6
        if c6:
            met += 1
            parts.append(f"RSI в норме ({ind.rsi:.0f})")

        conf = 0.0
        if met >= _MIN_MET:
            conf = 40 + max(0, met - 4) * 10
            if touch_count >= 4: conf += 5

        if met > best["met"] or (met >= _MIN_MET and conf > best["conf"]):
            best = {"met": met, "conf": conf, "reason": " | ".join(parts), "conds": conds}

    return best


# ── Helpers ───────────────────────────────────────────────────────────────────

def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "level_breakout", reason, extra or {})
