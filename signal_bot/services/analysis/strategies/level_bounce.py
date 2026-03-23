"""
Strategy 3 — Level Bounce (1m levels + 15s entry)

STEP 1: Find levels on 1m candles (last 50 bars).
        Prices where candle highs/lows cluster within 0.03% = level.
        Touch count = how many pivots landed in the cluster.

STEP 2: Detect reaction on 15s candles.
        Price approaching 1m level + rejection candle pattern.

Need 4 of 6 conditions. Base confidence: 40 + (met-4)*10.
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


_TOTAL        = 6
_MIN_MET      = 4
_CLUSTER_PCT  = 0.0003   # 0.03% — cluster radius for grouping nearby pivots
_APPROACH_PCT = 0.0005   # 0.05% — price is "at level" within this distance


# ── 1m level detection ────────────────────────────────────────────────────────

def find_1m_levels(df1m: pd.DataFrame) -> tuple[list[tuple[float, int]], list[tuple[float, int]]]:
    """
    Scan last 50 1m candles for S/R levels.
    Returns (supports, resistances) as list of (price, touch_count) tuples,
    sorted by touch_count descending.
    """
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


# ── Main entry point ──────────────────────────────────────────────────────────

def level_bounce_strategy(
    df: pd.DataFrame,                       # 15s candles (entry timing)
    ind: Indicators,
    levels: LevelSet,                       # kept for API compatibility
    df1m_ctx: pd.DataFrame | None = None,   # 1m candles (level detection)
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
) -> StrategyResult:
    if df1m_ctx is None or len(df1m_ctx) < 10:
        return _none("Нет 1m данных для уровней", {"early_reject": "no_1m_data"})

    n = len(df)
    if n < 6:
        return _none("Мало 15s данных", {"early_reject": "n<6"})

    if ind.atr_ratio < 0.30:
        return _none("ATR мёртвый",
                     {"early_reject": f"atr_ratio={round(ind.atr_ratio,3)}<0.30"})

    close    = df["close"].values
    open_    = df["open"].values
    high     = df["high"].values
    low      = df["low"].values
    price    = float(close[-1])
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    # STEP 1 — find 1m levels
    sup_levels, res_levels = find_1m_levels(df1m_ctx)

    # STEP 2 — evaluate each direction (always returns full condition set for debug)
    best_buy  = _eval_buy(close, open_, high, low, n, price, avg_body, ind,
                          sup_levels, res_levels, mode, ctx_trend_up)
    best_sell = _eval_sell(close, open_, high, low, n, price, avg_body, ind,
                           res_levels, sup_levels, mode, ctx_trend_down)

    direction      = "NONE"
    conditions_met = 0
    base_conf      = 0.0
    reason         = "Уровень не найден или нет паттерна"

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
        strategy_name="level_bounce",
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


# ── BUY: bounce from 1m support ───────────────────────────────────────────────

def _eval_buy(close, open_, high, low, n, price, avg_body, ind,
              sup_levels, res_levels, mode, ctx_confirms: bool = False) -> dict:
    """
    Evaluate all 6 conditions for every candidate support level.
    Always returns the best partial result (most conditions met),
    even if below _MIN_MET — so debug always shows condition checkmarks.
    ctx_confirms: True if 1m MTF trend agrees with BUY direction.
    """
    best: dict = {"met": 0, "conf": 0.0, "reason": "", "conds": {}}

    for sup_price, touch_count in sup_levels[:5]:
        if touch_count < 2:
            continue

        conds: dict[str, bool] = {}
        met   = 0
        parts: list[str] = []

        # 1. Strong 1m level: 2+ touches
        c1 = True  # already filtered above
        conds["strong_1m_level"] = c1
        met += 1
        parts.append(f"Уровень 1m {sup_price:.5f} ({touch_count}x)")

        # 2. Price at level: close or low of last 4 15s candles within 0.05%
        c2 = any(
            abs(float(low[-i])   - sup_price) / sup_price < _APPROACH_PCT or
            abs(float(close[-i]) - sup_price) / sup_price < _APPROACH_PCT
            for i in range(1, min(5, n))
        )
        conds["price_at_level"] = c2
        if c2:
            met += 1
            parts.append("Цена у уровня (±0.05%)")

        # 3. Rejection candle 15s: lower wick > 2x body
        body     = abs(float(close[-1]) - float(open_[-1]))
        wick_low = min(float(close[-1]), float(open_[-1])) - float(low[-1])
        c3 = (wick_low > 2.0 * body) if body > 1e-10 else (wick_low > avg_body * 0.5)
        conds["rejection_candle_15s"] = c3
        if c3:
            ratio = wick_low / avg_body if avg_body > 0 else 0
            met += 1
            parts.append(f"Отскок-свеча (тень {ratio:.1f}x avg)")

        # 4. RSI extreme: RSI < 40
        c4 = ind.rsi < 40
        conds["rsi_extreme_15s"] = c4
        if c4:
            met += 1
            parts.append(f"RSI перепродан ({ind.rsi:.0f})")

        # 5. Stoch turning up from low
        c5 = ind.stoch_k < 30 and ind.stoch_k > ind.stoch_k_prev
        conds["stoch_confirm"] = c5
        if c5:
            met += 1
            parts.append(f"Stoch разворот вверх ({ind.stoch_k:.0f})")

        # 6. Room to target: > 0.025% to nearest resistance
        nearest_res = res_levels[0][0] if res_levels else None
        room = (nearest_res - price) / price * 100 if (nearest_res and nearest_res > price) else 0.0
        c6 = room > 0.025
        conds["room_to_target"] = c6
        if c6:
            met += 1
            parts.append(f"Пространство {room:.2f}%")

        # Compute confidence (only meaningful when met >= _MIN_MET)
        conf = 0.0
        if met >= _MIN_MET:
            conf = 40 + max(0, met - 4) * 10
            if touch_count >= 3: conf += 5
            if mode == "RANGE":  conf += 5   # range bonus
            if ctx_confirms:     conf += 5   # 1m MTF trend confirms direction
            # Precision level touch bonus: price within 0.01% of support → +10
            # Boosts level_bounce over conflicting ema_bounce when price is exactly at level
            dist_pct = min(
                abs(float(close[-1]) - sup_price) / sup_price,
                abs(float(low[-1])   - sup_price) / sup_price,
            )
            if dist_pct < 0.0001:  # < 0.01%
                conf += 10
                parts.append("Точное касание уровня (<0.01%) +10")

        # Always update best if this level has more conditions met
        # (so debug always shows full condition set, even below threshold)
        if met > best["met"] or (met >= _MIN_MET and conf > best["conf"]):
            best = {"met": met, "conf": conf, "reason": " | ".join(parts), "conds": conds}

    return best


# ── SELL: bounce from 1m resistance ──────────────────────────────────────────

def _eval_sell(close, open_, high, low, n, price, avg_body, ind,
               res_levels, sup_levels, mode, ctx_confirms: bool = False) -> dict:
    """
    Evaluate all 6 conditions for every candidate resistance level.
    Always returns the best partial result for debug visibility.
    ctx_confirms: True if 1m MTF trend agrees with SELL direction.
    """
    best: dict = {"met": 0, "conf": 0.0, "reason": "", "conds": {}}

    for res_price, touch_count in res_levels[:5]:
        if touch_count < 2:
            continue

        conds: dict[str, bool] = {}
        met   = 0
        parts: list[str] = []

        # 1. Strong 1m level
        c1 = True
        conds["strong_1m_level"] = c1
        met += 1
        parts.append(f"Сопротивление 1m {res_price:.5f} ({touch_count}x)")

        # 2. Price at level: close or high of last 4 15s candles within 0.05%
        c2 = any(
            abs(float(high[-i])  - res_price) / res_price < _APPROACH_PCT or
            abs(float(close[-i]) - res_price) / res_price < _APPROACH_PCT
            for i in range(1, min(5, n))
        )
        conds["price_at_level"] = c2
        if c2:
            met += 1
            parts.append("Цена у уровня (±0.05%)")

        # 3. Rejection candle 15s: upper wick > 2x body
        body      = abs(float(close[-1]) - float(open_[-1]))
        wick_high = float(high[-1]) - max(float(close[-1]), float(open_[-1]))
        c3 = (wick_high > 2.0 * body) if body > 1e-10 else (wick_high > avg_body * 0.5)
        conds["rejection_candle_15s"] = c3
        if c3:
            ratio = wick_high / avg_body if avg_body > 0 else 0
            met += 1
            parts.append(f"Отскок-свеча (тень {ratio:.1f}x avg)")

        # 4. RSI extreme: RSI > 60
        c4 = ind.rsi > 60
        conds["rsi_extreme_15s"] = c4
        if c4:
            met += 1
            parts.append(f"RSI перекуплен ({ind.rsi:.0f})")

        # 5. Stoch turning down from high
        c5 = ind.stoch_k > 70 and ind.stoch_k < ind.stoch_k_prev
        conds["stoch_confirm"] = c5
        if c5:
            met += 1
            parts.append(f"Stoch разворот вниз ({ind.stoch_k:.0f})")

        # 6. Room to target: > 0.025% to nearest support
        nearest_sup = sup_levels[0][0] if sup_levels else None
        room = (price - nearest_sup) / price * 100 if (nearest_sup and nearest_sup < price) else 0.0
        c6 = room > 0.025
        conds["room_to_target"] = c6
        if c6:
            met += 1
            parts.append(f"Пространство {room:.2f}%")

        conf = 0.0
        if met >= _MIN_MET:
            conf = 40 + max(0, met - 4) * 10
            if touch_count >= 3: conf += 5
            if mode == "RANGE":  conf += 5   # range bonus
            if ctx_confirms:     conf += 5   # 1m MTF trend confirms direction
            # Precision level touch bonus: price within 0.01% of resistance → +10
            dist_pct = min(
                abs(float(close[-1]) - res_price) / res_price,
                abs(float(high[-1])  - res_price) / res_price,
            )
            if dist_pct < 0.0001:  # < 0.01%
                conf += 10
                parts.append("Точное касание уровня (<0.01%) +10")

        if met > best["met"] or (met >= _MIN_MET and conf > best["conf"]):
            best = {"met": met, "conf": conf, "reason": " | ".join(parts), "conds": conds}

    return best


# ── Helpers ───────────────────────────────────────────────────────────────────

def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "level_bounce", reason, extra or {})
