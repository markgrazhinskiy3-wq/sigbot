"""
Strategy 1 — RSI + Bollinger Scalp
Philosophy: OTC prices mean-revert after deviation. Catch the bounce from BB band
when RSI confirms extreme exhaustion.

Entry:
  CALL: price touches/breaks BB lower + RSI < 30 + first bullish confirmation candle
  PUT:  price touches/breaks BB upper + RSI > 70 + first bearish confirmation candle

Expiry: 1 minute
Best in: RANGE, SQUEEZE (also VOLATILE with caution)
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
_MIN_MET = 4


def rsi_bb_scalp_strategy(
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

    if n < 20:
        return _none("Мало данных", {"early_reject": "n<20"})

    # Dead market guard
    if ind.atr_ratio < 0.3:
        return _none("ATR мёртвый", {"early_reject": f"atr_ratio={ind.atr_ratio:.3f}<0.3"})

    # Compute BB(20, 2.0) inline — more accurate than BB(15) from indicators
    bb_p  = min(20, n)
    close_s = pd.Series(close)
    bb_mid_s = close_s.rolling(bb_p).mean()
    bb_std_s = close_s.rolling(bb_p).std()
    bb_upper = float((bb_mid_s + 2.0 * bb_std_s).iloc[-1])
    bb_lower = float((bb_mid_s - 2.0 * bb_std_s).iloc[-1])
    bb_mid   = float(bb_mid_s.iloc[-1])

    # BB bandwidth — detect if bands are exploding (high volatility, skip)
    bb_bw_now  = (bb_upper - bb_lower) / bb_mid if bb_mid else 0.0
    bb_bw_prev = 0.0
    if n >= bb_p + 5:
        bb_bw_prev = float((bb_mid_s + 2.0 * bb_std_s).iloc[-6] - (bb_mid_s - 2.0 * bb_std_s).iloc[-6])
        bb_bw_prev = bb_bw_prev / float(bb_mid_s.iloc[-6]) if float(bb_mid_s.iloc[-6]) else bb_bw_now
    bb_expanding = bb_bw_now > bb_bw_prev * 1.15  # BB widened >15% vs 5 bars ago

    price = close[-1]
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    buy_met, buy_parts, buy_conds = _check_buy(
        close, open_, high, low, n, price, avg_body, ind,
        bb_lower, bb_mid, bb_upper, bb_expanding,
    )
    sell_met, sell_parts, sell_conds = _check_sell(
        close, open_, high, low, n, price, avg_body, ind,
        bb_lower, bb_mid, bb_upper, bb_expanding,
    )

    # Pick direction — mean-reversion works against trends; prefer the one more confirmed
    buy_wins  = buy_met > sell_met or (buy_met == sell_met and ctx_trend_down)
    sell_wins = sell_met > buy_met or (sell_met == buy_met and ctx_trend_up)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Условия не выполнены"
    dbg_conds = {}

    if buy_wins and buy_met >= _MIN_MET:
        direction = "BUY"
        conditions_met = buy_met
        base_conf = 55 + (buy_met - _MIN_MET) * 9
        reason = " | ".join(buy_parts)
        dbg_conds = buy_conds
        # Strong signal: RSI deeply oversold
        if ind.rsi < 20:
            base_conf += 8
            reason += " | RSI<20 экстремум (+8)"
        # BB not too wide (narrow/medium bands = better signal quality)
        if bb_bw_now < 0.005:
            base_conf += 5
            reason += " | BB узкие (+5)"

    elif sell_wins and sell_met >= _MIN_MET:
        direction = "SELL"
        conditions_met = sell_met
        base_conf = 55 + (sell_met - _MIN_MET) * 9
        reason = " | ".join(sell_parts)
        dbg_conds = sell_conds
        if ind.rsi > 80:
            base_conf += 8
            reason += " | RSI>80 экстремум (+8)"
        if bb_bw_now < 0.005:
            base_conf += 5
            reason += " | BB узкие (+5)"

    # Block in strong trend (mean-reversion is risky)
    if direction == "BUY" and mode == "TRENDING_DOWN":
        return _none(
            "BUY заблокирован: TRENDING_DOWN (ловля ножа)",
            {"trend_block": "buy_in_downtrend", "buy_met": buy_met, "sell_met": sell_met,
             "buy_conditions": buy_conds, "sell_conditions": sell_conds},
        )
    if direction == "SELL" and mode == "TRENDING_UP":
        return _none(
            "SELL заблокирован: TRENDING_UP (ловля ножа)",
            {"trend_block": "sell_in_uptrend", "buy_met": buy_met, "sell_met": sell_met,
             "buy_conditions": buy_conds, "sell_conditions": sell_conds},
        )

    if direction == "NONE":
        return _none(reason, {
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds, "sell_conditions": sell_conds,
            "bb_lower": round(bb_lower, 6), "bb_upper": round(bb_upper, 6),
            "bb_bw": round(bb_bw_now, 5),
        })

    return StrategyResult(
        direction=direction,
        confidence=max(0.0, min(100.0, base_conf)),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="rsi_bb_scalp",
        reasoning=reason,
        debug={
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds if direction == "BUY" else {},
            "sell_conditions": sell_conds if direction == "SELL" else {},
            "bb_lower": round(bb_lower, 6), "bb_upper": round(bb_upper, 6),
            "bb_mid": round(bb_mid, 6), "bb_bw": round(bb_bw_now, 5),
            "bb_expanding": bb_expanding,
        }
    )


def _check_buy(close, open_, high, low, n, price, avg_body, ind,
               bb_lower, bb_mid, bb_upper, bb_expanding):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # C1: Price touched or broke BB lower in last 2 candles
    c1 = any(float(low[-i]) <= bb_lower * 1.001 for i in range(1, min(3, n)))
    conds["price_at_bb_lower"] = c1
    if c1:
        met += 1; parts.append(f"Цена у нижней BB ({bb_lower:.6f})")

    # C2: RSI < 30 (oversold)
    c2 = ind.rsi < 30
    conds["rsi_oversold"] = c2
    if c2:
        met += 1; parts.append(f"RSI перепроданность ({ind.rsi:.1f}<30)")

    # C3: Current candle is bullish (close > open) — confirmation candle
    c3 = close[-1] > open_[-1]
    conds["bullish_candle"] = c3
    if c3:
        met += 1; parts.append("Бычья свеча подтверждения")

    # C4: Confirmation candle body >= 30% of total range (not doji)
    total_range = high[-1] - low[-1]
    body_pct = abs(close[-1] - open_[-1]) / total_range if total_range > 0 else 0
    c4 = body_pct >= 0.30
    conds["candle_not_doji"] = c4
    if c4:
        met += 1; parts.append(f"Тело свечи {body_pct*100:.0f}% (≥30%)")

    # C5: BB is not explosively expanding
    c5 = not bb_expanding
    conds["bb_not_expanding"] = c5
    if c5:
        met += 1; parts.append("BB не расширяется резко")

    # C6: Price bounced off — current close above BB lower (not still below)
    c6 = close[-1] > bb_lower
    conds["price_above_bb_lower"] = c6
    if c6:
        met += 1; parts.append("Цена отскочила выше BB нижней")

    return met, parts, conds


def _check_sell(close, open_, high, low, n, price, avg_body, ind,
                bb_lower, bb_mid, bb_upper, bb_expanding):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # C1: Price touched or broke BB upper in last 2 candles
    c1 = any(float(high[-i]) >= bb_upper * 0.999 for i in range(1, min(3, n)))
    conds["price_at_bb_upper"] = c1
    if c1:
        met += 1; parts.append(f"Цена у верхней BB ({bb_upper:.6f})")

    # C2: RSI > 70 (overbought)
    c2 = ind.rsi > 70
    conds["rsi_overbought"] = c2
    if c2:
        met += 1; parts.append(f"RSI перекупленность ({ind.rsi:.1f}>70)")

    # C3: Current candle is bearish — confirmation candle
    c3 = close[-1] < open_[-1]
    conds["bearish_candle"] = c3
    if c3:
        met += 1; parts.append("Медвежья свеча подтверждения")

    # C4: Confirmation candle body >= 30% of total range (not doji)
    total_range = high[-1] - low[-1]
    body_pct = abs(close[-1] - open_[-1]) / total_range if total_range > 0 else 0
    c4 = body_pct >= 0.30
    conds["candle_not_doji"] = c4
    if c4:
        met += 1; parts.append(f"Тело свечи {body_pct*100:.0f}% (≥30%)")

    # C5: BB is not explosively expanding
    c5 = not bb_expanding
    conds["bb_not_expanding"] = c5
    if c5:
        met += 1; parts.append("BB не расширяется резко")

    # C6: Price fell back below BB upper (not still above)
    c6 = close[-1] < bb_upper
    conds["price_below_bb_upper"] = c6
    if c6:
        met += 1; parts.append("Цена отскочила ниже BB верхней")

    return met, parts, conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "rsi_bb_scalp", reason, extra or {})
