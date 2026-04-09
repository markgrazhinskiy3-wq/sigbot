"""
Strategy 3 — Stochastic OTC Snap
Philosophy: Stochastic oscillator crossovers in extreme zones signal exhaustion
and reversal. On OTC markets where price mean-reverts, this works well.

Entry:
  CALL: Both %K and %D below 20, %K crosses %D upward (in oversold zone)
  PUT:  Both %K and %D above 80, %K crosses %D downward (in overbought zone)

Strong signal: lines deeply in zone (<10 or >90), sharp angle of crossing
Expiry: 1 minute
Best in: RANGE, SQUEEZE, VOLATILE
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


_TOTAL   = 4
_MIN_MET = 3


def stoch_snap_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
) -> StrategyResult:
    close = df["close"].values
    open_ = df["open"].values
    n     = len(df)

    if n < 10:
        return _none("Мало данных", {"early_reject": "n<10"})

    if ind.atr_ratio < 0.25:
        return _none("ATR мёртвый", {"early_reject": f"atr_ratio={ind.atr_ratio:.3f}<0.25"})

    # Stochastic values from indicators (5, 3, 3)
    k_now  = ind.stoch_k
    d_now  = ind.stoch_d
    k_prev = ind.stoch_k_prev  # previous smoothed %K

    # Approximate %D previous: since D is a 3-bar SMA of K, and we only have
    # k_prev, we use k_now and k_prev relationship to infer crossover direction
    # Cross up: k_prev < d_now and k_now > d_now (K just crossed D from below)
    # Cross dn: k_prev > d_now and k_now < d_now (K just crossed D from above)
    cross_up = k_prev < d_now and k_now > d_now
    cross_dn = k_prev > d_now and k_now < d_now

    # Additional: K was moving up/down (momentum of K itself)
    k_rising  = k_now > k_prev
    k_falling = k_now < k_prev

    buy_met, buy_parts, buy_conds = _check_buy(k_now, d_now, k_prev, cross_up, k_rising, ind)
    sell_met, sell_parts, sell_conds = _check_sell(k_now, d_now, k_prev, cross_dn, k_falling, ind)

    buy_wins  = buy_met > sell_met or (buy_met == sell_met and ctx_trend_down)
    sell_wins = sell_met > buy_met or (sell_met == buy_met and ctx_trend_up)

    direction = "NONE"
    conditions_met = 0
    base_conf = 0.0
    reason = "Условия не выполнены"

    if buy_wins and buy_met >= _MIN_MET:
        direction = "BUY"
        conditions_met = buy_met
        base_conf = 60 + (buy_met - _MIN_MET) * 12
        reason = " | ".join(buy_parts)
        # Deep zone bonus
        if k_now < 10 and d_now < 10:
            base_conf += 8
            reason += f" | Stoch глубоко <10 (+8)"
        # K sharply turned (big jump from prev)
        if k_now - k_prev > 8:
            base_conf += 5
            reason += f" | Резкий разворот K (+5)"

    elif sell_wins and sell_met >= _MIN_MET:
        direction = "SELL"
        conditions_met = sell_met
        base_conf = 60 + (sell_met - _MIN_MET) * 12
        reason = " | ".join(sell_parts)
        if k_now > 90 and d_now > 90:
            base_conf += 8
            reason += f" | Stoch глубоко >90 (+8)"
        if k_prev - k_now > 8:
            base_conf += 5
            reason += f" | Резкий разворот K (+5)"

    # Block in strong trend — stoch in extreme for too long = trend, not exhaustion
    if direction == "BUY" and mode == "TRENDING_DOWN":
        return _none(
            "BUY заблокирован: TRENDING_DOWN",
            {"trend_block": True, "buy_met": buy_met, "sell_met": sell_met,
             "buy_conditions": buy_conds, "sell_conditions": sell_conds},
        )
    if direction == "SELL" and mode == "TRENDING_UP":
        return _none(
            "SELL заблокирован: TRENDING_UP",
            {"trend_block": True, "buy_met": buy_met, "sell_met": sell_met,
             "buy_conditions": buy_conds, "sell_conditions": sell_conds},
        )

    if direction == "NONE":
        return _none(reason, {
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds, "sell_conditions": sell_conds,
            "stoch_k": round(k_now, 1), "stoch_d": round(d_now, 1),
            "stoch_k_prev": round(k_prev, 1),
        })

    return StrategyResult(
        direction=direction,
        confidence=max(0.0, min(100.0, base_conf)),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="stoch_snap",
        reasoning=reason,
        debug={
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds if direction == "BUY" else {},
            "sell_conditions": sell_conds if direction == "SELL" else {},
            "stoch_k": round(k_now, 1), "stoch_d": round(d_now, 1),
            "stoch_k_prev": round(k_prev, 1),
            "cross_up": cross_up, "cross_dn": cross_dn,
        }
    )


def _check_buy(k_now, d_now, k_prev, cross_up, k_rising, ind):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # C1: %K is below 20 (oversold zone)
    c1 = k_now < 20
    conds["stoch_k_oversold"] = c1
    if c1:
        met += 1; parts.append(f"Stoch K={k_now:.1f} в зоне перепроданности (<20)")

    # C2: %D is below 20 (confirming oversold)
    c2 = d_now < 20
    conds["stoch_d_oversold"] = c2
    if c2:
        met += 1; parts.append(f"Stoch D={d_now:.1f} в зоне перепроданности (<20)")

    # C3: %K crossed %D upward (K was below D, now above D)
    c3 = cross_up or (k_rising and k_now > d_now and k_prev <= d_now * 1.05)
    conds["k_crossed_d_up"] = c3
    if c3:
        met += 1; parts.append(f"K пересёк D снизу вверх ({k_prev:.1f}→{k_now:.1f} vs D={d_now:.1f})")

    # C4: RSI confirms oversold (below 40 for extra confirmation)
    c4 = ind.rsi < 40
    conds["rsi_confirms_oversold"] = c4
    if c4:
        met += 1; parts.append(f"RSI {ind.rsi:.1f} подтверждает перепроданность (<40)")

    return met, parts, conds


def _check_sell(k_now, d_now, k_prev, cross_dn, k_falling, ind):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # C1: %K is above 80 (overbought zone)
    c1 = k_now > 80
    conds["stoch_k_overbought"] = c1
    if c1:
        met += 1; parts.append(f"Stoch K={k_now:.1f} в зоне перекупленности (>80)")

    # C2: %D is above 80
    c2 = d_now > 80
    conds["stoch_d_overbought"] = c2
    if c2:
        met += 1; parts.append(f"Stoch D={d_now:.1f} в зоне перекупленности (>80)")

    # C3: %K crossed %D downward
    c3 = cross_dn or (k_falling and k_now < d_now and k_prev >= d_now * 0.95)
    conds["k_crossed_d_dn"] = c3
    if c3:
        met += 1; parts.append(f"K пересёк D сверху вниз ({k_prev:.1f}→{k_now:.1f} vs D={d_now:.1f})")

    # C4: RSI confirms overbought (above 60)
    c4 = ind.rsi > 60
    conds["rsi_confirms_overbought"] = c4
    if c4:
        met += 1; parts.append(f"RSI {ind.rsi:.1f} подтверждает перекупленность (>60)")

    return met, parts, conds


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "stoch_snap", reason, extra or {})
