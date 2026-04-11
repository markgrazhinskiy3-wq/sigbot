"""
Strategy 4 — EMA Micro-Cross
Philosophy: Two fast EMAs (3, 8) react to micro-trends. When the fast one crosses
the slow one, a short-term movement is forming. RSI(10) filters out weak signals.

Entry:
  CALL: EMA(3) crosses EMA(8) upward + RSI > 50
  PUT:  EMA(3) crosses EMA(8) downward + RSI < 50

Filters: no choppy EMA (multiple recent crosses = flat), no doji candle, RSI not near 50
Expiry: 1 minute
Best in: TRENDING_UP, TRENDING_DOWN, RANGE with direction bias
"""
from __future__ import annotations
try:
    from ..pair_profile import PairParams
except ImportError:
    PairParams = None  # type: ignore[misc,assignment]
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


_TOTAL   = 5
_MIN_MET = 4   # raised from 3 → needs 4/5 conditions (dominated 46% of all trades at 51% WR)


def ema_micro_cross_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
    pair_params=None,
) -> StrategyResult:
    close = df["close"].values
    open_ = df["open"].values
    high  = df["high"].values
    low   = df["low"].values
    n     = len(df)

    # Pair-adapted EMA spans (volatile pairs use 5/13 instead of 3/8)
    fast_span = pair_params.ema_fast_span if pair_params else 3
    slow_span = pair_params.ema_slow_span if pair_params else 8

    if n < 12:
        return _none("Мало данных", {"early_reject": "n<12"})

    if ind.atr_ratio < 0.25:
        return _none("ATR мёртвый", {"early_reject": f"atr_ratio={ind.atr_ratio:.3f}<0.25"})

    close_s = pd.Series(close)

    # Compute EMA(fast) and EMA(slow) inline — pair-adapted spans
    ema3_s = close_s.ewm(span=fast_span, adjust=False).mean()
    ema8_s = close_s.ewm(span=slow_span, adjust=False).mean()

    ema3_now  = float(ema3_s.iloc[-1])
    ema8_now  = float(ema8_s.iloc[-1])
    ema3_prev = float(ema3_s.iloc[-2]) if n >= 2 else ema3_now
    ema8_prev = float(ema8_s.iloc[-2]) if n >= 2 else ema8_now

    # Cross detection
    cross_up = ema3_prev <= ema8_prev and ema3_now > ema8_now
    cross_dn = ema3_prev >= ema8_prev and ema3_now < ema8_now

    # Also accept "recently crossed" within last 2 bars
    if not cross_up and n >= 3:
        cross_up = (float(ema3_s.iloc[-3]) <= float(ema8_s.iloc[-3]) and
                    float(ema3_s.iloc[-2]) > float(ema8_s.iloc[-2]))
    if not cross_dn and n >= 3:
        cross_dn = (float(ema3_s.iloc[-3]) >= float(ema8_s.iloc[-3]) and
                    float(ema3_s.iloc[-2]) < float(ema8_s.iloc[-2]))

    # Choppy EMA detection: count cross-overs in last 10 bars
    chop_count = 0
    for i in range(2, min(11, n)):
        prev_above = float(ema3_s.iloc[-i-1]) > float(ema8_s.iloc[-i-1])
        curr_above = float(ema3_s.iloc[-i])   > float(ema8_s.iloc[-i])
        if prev_above != curr_above:
            chop_count += 1
    is_choppy = chop_count >= 2  # tightened: 2+ crossovers = EMA entangled (was 3)

    price    = close[-1]
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    buy_met, buy_parts, buy_conds = _check_buy(
        close, open_, high, low, n, avg_body, ind,
        cross_up, ema3_now, ema8_now, is_choppy
    )
    sell_met, sell_parts, sell_conds = _check_sell(
        close, open_, high, low, n, avg_body, ind,
        cross_dn, ema3_now, ema8_now, is_choppy
    )

    buy_wins  = buy_met > sell_met or (buy_met == sell_met and ctx_trend_up)
    sell_wins = sell_met > buy_met or (sell_met == buy_met and ctx_trend_down)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Условия не выполнены"

    if buy_wins and buy_met >= _MIN_MET:
        direction = "BUY"
        conditions_met = buy_met
        base_conf = 55 + (buy_met - _MIN_MET) * 8  # reduced from 58+10 → less inflation
        reason = " | ".join(buy_parts)
        # Strong: RSI clearly above 58 (tightened from 55)
        if ind.rsi > 58:
            base_conf += 5
            reason += f" | RSI {ind.rsi:.1f} чёткий бычий (+5)"
        # EMA distance growing (real trend, not noise)
        if abs(ema3_now - ema8_now) > abs(ema3_prev - ema8_prev):
            base_conf += 3
            reason += " | EMA расходятся (+3)"
        # RSI bullish divergence: cross with hidden strength → extra confirmation
        if ind.rsi_bull_div:
            base_conf += 5
            reason += " | RSI бычья дивергенция (+5)"

    elif sell_wins and sell_met >= _MIN_MET:
        direction = "SELL"
        conditions_met = sell_met
        base_conf = 55 + (sell_met - _MIN_MET) * 8
        reason = " | ".join(sell_parts)
        if ind.rsi < 42:  # tightened from 45
            base_conf += 5
            reason += f" | RSI {ind.rsi:.1f} чёткий медвежий (+5)"
        if abs(ema3_now - ema8_now) > abs(ema3_prev - ema8_prev):
            base_conf += 3
            reason += " | EMA расходятся (+3)"
        # RSI bearish divergence: cross with hidden weakness → extra confirmation
        if ind.rsi_bear_div:
            base_conf += 5
            reason += " | RSI медвежья дивергенция (+5)"

    if direction == "NONE":
        return _none(reason, {
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds, "sell_conditions": sell_conds,
            "cross_up": cross_up, "cross_dn": cross_dn,
            "ema3": round(ema3_now, 6), "ema8": round(ema8_now, 6),
            "is_choppy": is_choppy, "chop_count": chop_count,
        })

    return StrategyResult(
        direction=direction,
        confidence=max(0.0, min(100.0, base_conf)),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="ema_micro_cross",
        reasoning=reason,
        debug={
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds if direction == "BUY" else {},
            "sell_conditions": sell_conds if direction == "SELL" else {},
            "cross_up": cross_up, "cross_dn": cross_dn,
            "ema3": round(ema3_now, 6), "ema8": round(ema8_now, 6),
            "is_choppy": is_choppy, "chop_count": chop_count,
        }
    )


def _check_buy(close, open_, high, low, n, avg_body, ind,
               cross_up, ema3_now, ema8_now, is_choppy):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # C1: EMA(3) crossed EMA(8) upward (or recently crossed)
    conds["ema_cross_up"] = cross_up
    if cross_up:
        met += 1; parts.append(f"EMA(3) пересёк EMA(8) вверх")

    # C2: RSI > 50 (bullish momentum confirmed)
    c2 = ind.rsi > 50
    conds["rsi_above_50"] = c2
    if c2:
        met += 1; parts.append(f"RSI={ind.rsi:.1f} выше 50 (бычий)")

    # C3: Not choppy (EMA not entangled)
    c3 = not is_choppy
    conds["no_chop"] = c3
    if c3:
        met += 1; parts.append("EMA не переплетены")

    # C4: Current candle is not a doji (body >= 20% of range)
    total_range = high[-1] - low[-1]
    body_pct = abs(close[-1] - open_[-1]) / total_range if total_range > 0 else 0
    c4 = body_pct >= 0.20
    conds["candle_not_doji"] = c4
    if c4:
        met += 1; parts.append(f"Тело свечи {body_pct*100:.0f}% (не дожи)")

    # C5: RSI not borderline — widened neutral zone to 45-55 (was 48-52)
    c5 = ind.rsi < 45 or ind.rsi > 55
    conds["rsi_decisive"] = c5
    if c5:
        met += 1; parts.append(f"RSI {ind.rsi:.1f} вне нейтральной зоны 45-55")

    return met, parts, conds


def _check_sell(close, open_, high, low, n, avg_body, ind,
                cross_dn, ema3_now, ema8_now, is_choppy):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # C1: EMA(3) crossed EMA(8) downward
    conds["ema_cross_dn"] = cross_dn
    if cross_dn:
        met += 1; parts.append(f"EMA(3) пересёк EMA(8) вниз")

    # C2: RSI < 50
    c2 = ind.rsi < 50
    conds["rsi_below_50"] = c2
    if c2:
        met += 1; parts.append(f"RSI={ind.rsi:.1f} ниже 50 (медвежий)")

    # C3: Not choppy
    c3 = not is_choppy
    conds["no_chop"] = c3
    if c3:
        met += 1; parts.append("EMA не переплетены")

    # C4: Not a doji
    total_range = high[-1] - low[-1]
    body_pct = abs(close[-1] - open_[-1]) / total_range if total_range > 0 else 0
    c4 = body_pct >= 0.20
    conds["candle_not_doji"] = c4
    if c4:
        met += 1; parts.append(f"Тело свечи {body_pct*100:.0f}% (не дожи)")

    # C5: RSI decisive — widened neutral zone to 45-55 (was 48-52)
    c5 = ind.rsi < 45 or ind.rsi > 55
    conds["rsi_decisive"] = c5
    if c5:
        met += 1; parts.append(f"RSI {ind.rsi:.1f} вне нейтральной зоны 45-55")

    return met, parts, conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "ema_micro_cross", reason, extra or {})
