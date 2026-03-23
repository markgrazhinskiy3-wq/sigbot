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
_MIN_MET = 4


def ema_bounce_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
) -> StrategyResult:
    """
    Returns BUY / SELL / NONE with confidence 0-100.
    Requires >= 4 of 8 conditions met.

    Regime filter:
      TRENDING_UP   → only BUY (pullback INTO uptrend = BUY opportunity)
      TRENDING_DOWN → only SELL (pullback INTO downtrend = SELL opportunity)
      All others    → both directions allowed
    """
    close = df["close"].values
    open_ = df["open"].values
    high  = df["high"].values
    low   = df["low"].values
    n     = len(df)

    if n < 8:
        return _none("Мало данных", {"early_reject": "n<8"})

    # Hard reject: dead market (EMA bounces work in mild volatility, so lower bar than breakouts)
    if ind.atr_ratio < 0.35:
        return _none("ATR мёртвый — рынок стоит",
                     {"early_reject": f"atr_ratio={round(ind.atr_ratio,3)}<0.35",
                      "atr_ratio": round(ind.atr_ratio, 3)})

    price    = close[-1]
    avg_body = float(np.mean(np.abs(close[-min(10, n):] - open_[-min(10, n):]))) or 1e-8

    buy_score, buy_met, buy_parts, buy_conds   = _check_buy(close, open_, high, low, n, price, avg_body, ind)
    sell_score, sell_met, sell_parts, sell_conds = _check_sell(close, open_, high, low, n, price, avg_body, ind)

    direction      = "NONE"
    conditions_met = 0
    base_conf      = 0.0
    reason         = "Условия не выполнены"

    # Tiebreaker: if scores are equal, context trend breaks the tie
    buy_wins  = (buy_met > sell_met) or (buy_met == sell_met and ctx_trend_up  and not ctx_trend_down)
    sell_wins = (sell_met > buy_met) or (sell_met == buy_met and ctx_trend_down and not ctx_trend_up)

    if buy_wins and buy_met >= _MIN_MET:
        direction      = "BUY"
        conditions_met = buy_met
        # Anchored curve: 4→40, 5→50, 6→60, 7→70, 8→80
        base_conf      = 40 + max(0, buy_met - 4) * 10
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

    elif sell_wins and sell_met >= _MIN_MET:
        direction      = "SELL"
        conditions_met = sell_met
        # Same anchored curve as buy side: 4→40, 5→50, 6→60, 7→70, 8→80
        base_conf      = 40 + max(0, sell_met - 4) * 10
        reason         = " | ".join(sell_parts)
        if abs(high[-1] - ind.ema13) / price < 0.0001:
            base_conf += 5
        if abs(close[-1] - open_[-1]) > 0 and (close[-1] - low[-1]) < abs(close[-1] - open_[-1]) * 0.2:
            base_conf += 3
        if 40 <= ind.rsi <= 55:
            base_conf += 3

    # ── Trend momentum penalty (FIX 1) ───────────────────────────────────────
    # A weak bull/bear ratio contradicts the trend classification.
    # If fewer than half the recent candles are in trend direction → -15.
    if direction != "NONE":
        recent_n   = min(20, n)
        bull_pct   = float(np.sum(close[-recent_n:] > open_[-recent_n:])) / recent_n * 100
        bear_pct   = 100.0 - bull_pct
        if direction == "BUY"  and mode == "TRENDING_UP"   and bull_pct < 50:
            base_conf -= 15
            reason += f" | ⚠ Слабый тренд (бычьих {bull_pct:.0f}%) -15"
        if direction == "SELL" and mode == "TRENDING_DOWN"  and bear_pct < 50:
            base_conf -= 15
            reason += f" | ⚠ Слабый тренд (медвежьих {bear_pct:.0f}%) -15"

    # ── Level proximity penalty (FIX 2) ──────────────────────────────────────
    # Buying near resistance or selling near support is high risk.
    # If within 0.02% of the opposing level → -15.
    if direction == "BUY" and levels.resistances:
        above_res = [r for r in levels.resistances if r > price]
        if above_res:
            nearest_res = min(above_res)
            dist_pct = (nearest_res - price) / price
            if dist_pct < 0.0002:  # within 0.02%
                base_conf -= 15
                reason += f" | ⚠ BUY близко к сопротивлению ({dist_pct*100:.3f}%) -15"
    if direction == "SELL" and levels.supports:
        below_sup = [s for s in levels.supports if s < price]
        if below_sup:
            nearest_sup = max(below_sup)
            dist_pct = (price - nearest_sup) / price
            if dist_pct < 0.0002:  # within 0.02%
                base_conf -= 15
                reason += f" | ⚠ SELL близко к поддержке ({dist_pct*100:.3f}%) -15"

    # ── Exhaustion hard gate ──────────────────────────────────────────────────
    # If RSI/Stoch shows extreme exhaustion in the direction of the signal,
    # the move is already played out — block the signal immediately.
    # SELL when RSI<25 or StochK<10 → price is oversold, bounce likely.
    # BUY  when RSI>75 or StochK>90 → price is overbought, reversal likely.
    if direction == "SELL" and (ind.rsi < 25 or ind.stoch_k < 10):
        return _none(
            f"SELL заблокирован: перепроданность (RSI={ind.rsi:.1f}, Stoch K={ind.stoch_k:.1f})",
            {"exhaustion_block": "sell_oversold",
             "rsi": round(ind.rsi, 1), "stoch_k": round(ind.stoch_k, 1)}
        )
    if direction == "BUY" and (ind.rsi > 75 or ind.stoch_k > 90):
        return _none(
            f"BUY заблокирован: перекупленность (RSI={ind.rsi:.1f}, Stoch K={ind.stoch_k:.1f})",
            {"exhaustion_block": "buy_overbought",
             "rsi": round(ind.rsi, 1), "stoch_k": round(ind.stoch_k, 1)}
        )

    # ── Regime direction filter ───────────────────────────────────────────────
    # In a confirmed trend, ema_bounce should only trade WITH the trend.
    # A pullback in TRENDING_UP is a BUY opportunity, never SELL.
    # A pullback in TRENDING_DOWN is a SELL opportunity, never BUY.
    if direction == "SELL" and mode == "TRENDING_UP":
        return _none(
            f"SELL заблокирован: режим TRENDING_UP (откат = BUY возможность)",
            {"regime_block": "sell_in_uptrend",
             "sell_met": sell_met, "buy_met": buy_met,
             "sell_conditions": sell_conds, "buy_conditions": buy_conds}
        )
    if direction == "BUY" and mode == "TRENDING_DOWN":
        return _none(
            f"BUY заблокирован: режим TRENDING_DOWN (откат = SELL возможность)",
            {"regime_block": "buy_in_downtrend",
             "sell_met": sell_met, "buy_met": buy_met,
             "sell_conditions": sell_conds, "buy_conditions": buy_conds}
        )

    return StrategyResult(
        direction=direction,
        confidence=max(0.0, min(100.0, base_conf)),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="ema_bounce",
        reasoning=reason,
        debug={
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds,
            "sell_conditions": sell_conds,
        }
    )


def _check_buy(close, open_, high, low, n, price, avg_body, ind: Indicators):
    met   = 0
    parts = []
    conds: dict[str, bool] = {}
    check = min(5, n)

    # 1. EMA aligned up — relaxed: EMA5 > EMA13 in 3+ of 5 bars, OR EMA5 trending up + 2+ bars
    #    Also requires minimum EMA5-EMA21 spread ≥ 0.005% — flat EMAs don't count as aligned
    ema5_arr  = ind.ema5_series.iloc[-check:].values
    ema13_arr = ind.ema13_series.iloc[-check:].values
    ema21_arr = ind.ema21_series.iloc[-check:].values
    ema5_slope = float(ema5_arr[-1]) - float(ema5_arr[0])
    ema_spread_pct = abs(float(ema5_arr[-1]) - float(ema21_arr[-1])) / price if price else 0
    ema_has_spread = ema_spread_pct >= 0.00005   # 0.005% minimum spread
    c1 = ema_has_spread and (
        (int(np.sum(ema5_arr > ema13_arr)) >= 3) or
        (ema5_slope > 0 and int(np.sum(ema5_arr > ema13_arr)) >= 2)
    )
    conds["ema_aligned_up"] = c1
    if c1:
        met += 1; parts.append("EMA выровнены вверх")

    # 2. Price low touched EMA(13) zone — ±0.07% (widened for OTC spread tolerance)
    ema13_zone = ind.ema13 * 1.0007
    c2 = any(float(low[-i]) <= ema13_zone for i in range(1, min(4, n)))
    conds["price_near_ema13"] = c2
    if c2:
        met += 1; parts.append("Коснулась EMA13 (±0.07%)")

    # 3. Candle closed ABOVE EMA(13)
    c3 = close[-1] > ind.ema13
    conds["close_above_ema13"] = c3
    if c3:
        met += 1; parts.append("Закрылась выше EMA13")

    # 4. Bounce candle is bullish with body > 50% avg_body
    c4 = close[-1] > open_[-1] and abs(close[-1] - open_[-1]) > avg_body * 0.5
    conds["bounce_candle_bullish"] = c4
    if c4:
        met += 1; parts.append("Бычья свеча отскока")

    # 5. Real pullback: 3 of 4 previous candles bearish AND at least one closed below EMA13
    c5 = False
    if n >= 5:
        pb_window = range(2, min(6, n))
        pb_bearish = [close[-i] < open_[-i] or abs(close[-i] - open_[-i]) < avg_body * 0.5
                      for i in pb_window]
        pb_below_ema = any(float(close[-i]) < ind.ema13 for i in pb_window)
        c5 = sum(pb_bearish) >= 3 and pb_below_ema
    conds["real_pullback"] = c5
    if c5:
        met += 1; parts.append("Откат к EMA13 (3/4 свечи медвежьи, одна под EMA)")

    # 6. RSI(7) between 40 and 70 (not overheated, not oversold)
    c6 = 40 <= ind.rsi <= 70
    conds["rsi_ok"] = c6
    if c6:
        met += 1; parts.append(f"RSI {ind.rsi:.0f} в норме")

    # 7. Stochastic %K > %D (relaxed: no strict cross required)
    c7 = ind.stoch_k > ind.stoch_d
    conds["stoch_turning_up"] = c7
    if c7:
        met += 1; parts.append(f"Stoch K>D ({ind.stoch_k:.0f}>{ind.stoch_d:.0f})")

    # 8. Bounce candle shows conviction: body > 0.5× avg AND closes in upper 50% of range
    total_range = high[-1] - low[-1]
    c8 = (abs(close[-1] - open_[-1]) > avg_body * 0.5 and
          total_range > 0 and (close[-1] - low[-1]) / total_range > 0.5)
    conds["candle_conviction"] = c8
    if c8:
        met += 1; parts.append("Свеча закрылась в верхней зоне диапазона")

    return met, met, parts, conds


def _check_sell(close, open_, high, low, n, price, avg_body, ind: Indicators):
    met   = 0
    parts = []
    conds: dict[str, bool] = {}
    check = min(5, n)

    # 1. EMA aligned down — relaxed: EMA5 < EMA13 in 3+ of 5 bars, OR EMA5 trending down + 2+ bars
    #    Also requires minimum EMA5-EMA21 spread ≥ 0.005% — flat EMAs don't count as aligned
    ema5_arr  = ind.ema5_series.iloc[-check:].values
    ema13_arr = ind.ema13_series.iloc[-check:].values
    ema21_arr = ind.ema21_series.iloc[-check:].values
    ema5_slope = float(ema5_arr[-1]) - float(ema5_arr[0])
    ema_spread_pct = abs(float(ema5_arr[-1]) - float(ema21_arr[-1])) / price if price else 0
    ema_has_spread = ema_spread_pct >= 0.00005   # 0.005% minimum spread
    c1 = ema_has_spread and (
        (int(np.sum(ema5_arr < ema13_arr)) >= 3) or
        (ema5_slope < 0 and int(np.sum(ema5_arr < ema13_arr)) >= 2)
    )
    conds["ema_aligned_down"] = c1
    if c1:
        met += 1; parts.append("EMA выровнены вниз")

    # 2. Price high touched EMA(13) zone — ±0.07% (widened for OTC spread tolerance)
    ema13_zone = ind.ema13 * 0.9993
    c2 = any(float(high[-i]) >= ema13_zone for i in range(1, min(4, n)))
    conds["price_near_ema13"] = c2
    if c2:
        met += 1; parts.append("Коснулась EMA13 сверху (±0.07%)")

    # 3. Candle closed BELOW EMA(13)
    c3 = close[-1] < ind.ema13
    conds["close_below_ema13"] = c3
    if c3:
        met += 1; parts.append("Закрылась ниже EMA13")

    # 4. Bounce candle is bearish with body > 50% avg_body
    c4 = close[-1] < open_[-1] and abs(close[-1] - open_[-1]) > avg_body * 0.5
    conds["bounce_candle_bearish"] = c4
    if c4:
        met += 1; parts.append("Медвежья свеча отскока")

    # 5. Real pullback: 3 of 4 previous candles bullish AND at least one closed above EMA13
    c5 = False
    if n >= 5:
        pb_window = range(2, min(6, n))
        pb_bullish = [close[-i] > open_[-i] or abs(close[-i] - open_[-i]) < avg_body * 0.5
                      for i in pb_window]
        pb_above_ema = any(float(close[-i]) > ind.ema13 for i in pb_window)
        c5 = sum(pb_bullish) >= 3 and pb_above_ema
    conds["real_pullback"] = c5
    if c5:
        met += 1; parts.append("Откат к EMA13 (3/4 свечи бычьи, одна над EMA)")

    # 6. RSI(7) between 30 and 60
    c6 = 30 <= ind.rsi <= 60
    conds["rsi_ok"] = c6
    if c6:
        met += 1; parts.append(f"RSI {ind.rsi:.0f} в норме")

    # 7. Stochastic %K < %D (relaxed: no strict cross required)
    c7 = ind.stoch_k < ind.stoch_d
    conds["stoch_turning_down"] = c7
    if c7:
        met += 1; parts.append(f"Stoch K<D ({ind.stoch_k:.0f}<{ind.stoch_d:.0f})")

    # 8. Bounce candle shows conviction: body > 0.5× avg AND closes in lower 50% of range
    total_range = high[-1] - low[-1]
    c8 = (abs(close[-1] - open_[-1]) > avg_body * 0.5 and
          total_range > 0 and (high[-1] - close[-1]) / total_range > 0.5)
    conds["candle_conviction"] = c8
    if c8:
        met += 1; parts.append("Свеча закрылась в нижней зоне диапазона")

    return met, met, parts, conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "ema_bounce", reason,
                          extra or {})
