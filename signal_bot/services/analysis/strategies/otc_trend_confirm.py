"""
Strategy 5 — OTC Trend Confirmation (MACD + RSI)
Philosophy: Trend-following strategy. MACD histogram crossing zero identifies a new
micro-trend, RSI(14) confirms its strength. Two-minute expiry gives the move time to develop.

Entry:
  CALL: MACD histogram crosses from negative to positive + MACD line above signal + RSI > 50
  PUT:  MACD histogram crosses from positive to negative + MACD line below signal + RSI < 50

RSI levels used as trend filter: >60 = strong bull, <40 = strong bear (NOT as OB/OS)
Expiry: 2 minutes
Best in: TRENDING_UP, TRENDING_DOWN, directional RANGE
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
_MIN_MET = 4


def otc_trend_confirm_strategy(
    df: pd.DataFrame,
    ind: Indicators,
    levels: LevelSet,
    ctx_trend_up: bool = False,
    ctx_trend_down: bool = False,
    mode: str = "RANGE",
    pair_params=None,
) -> StrategyResult:
    close = df["close"].values
    n     = len(df)

    if n < 30:
        return _none("Мало данных для MACD (нужно ≥30)", {"early_reject": "n<30"})

    if ind.atr_ratio < 0.3:
        return _none("ATR мёртвый", {"early_reject": f"atr_ratio={ind.atr_ratio:.3f}<0.3"})

    # MACD(12, 26, 9) computed inline
    close_s = pd.Series(close)
    ema12   = close_s.ewm(span=12, adjust=False).mean()
    ema26   = close_s.ewm(span=26, adjust=False).mean()
    macd    = ema12 - ema26
    signal  = macd.ewm(span=9, adjust=False).mean()
    hist    = macd - signal

    hist_now  = float(hist.iloc[-1])
    hist_prev = float(hist.iloc[-2]) if n >= 2 else hist_now
    macd_now  = float(macd.iloc[-1])
    sig_now   = float(signal.iloc[-1])

    # RSI(14) for trend filter (document specifies period 14 for this strategy)
    rsi14 = _rsi14(close_s)

    # Histogram zero crossing
    hist_cross_up = hist_prev < 0 and hist_now > 0  # negative → positive
    hist_cross_dn = hist_prev > 0 and hist_now < 0  # positive → negative

    # MACD flickering (crosses zero multiple times in last 6 bars = flat, ignore)
    flicker_count = 0
    for i in range(2, min(7, n)):
        if float(hist.iloc[-i]) * float(hist.iloc[-i-1]) < 0:
            flicker_count += 1
    is_flickering = flicker_count >= 2

    # Histogram growing (2nd bar bigger than 1st after crossing = strong signal)
    if n >= 3:
        hist_prev2 = float(hist.iloc[-3])
        hist_growing_up = hist_cross_up and abs(hist_now) > abs(hist_prev)
        hist_growing_dn = hist_cross_dn and abs(hist_now) > abs(hist_prev)
    else:
        hist_growing_up = hist_growing_dn = False

    macd_rsi_bull = pair_params.macd_rsi_bull if pair_params else 50.0
    macd_rsi_bear = pair_params.macd_rsi_bear if pair_params else 50.0

    buy_met, buy_parts, buy_conds = _check_buy(
        hist_now, hist_prev, hist_cross_up, macd_now, sig_now,
        rsi14, is_flickering, hist_growing_up, macd_rsi_bull,
    )
    sell_met, sell_parts, sell_conds = _check_sell(
        hist_now, hist_prev, hist_cross_dn, macd_now, sig_now,
        rsi14, is_flickering, hist_growing_dn, macd_rsi_bear,
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
        base_conf = 60 + (buy_met - _MIN_MET) * 10
        reason = " | ".join(buy_parts)
        # Strong trend confirmation
        if rsi14 > 60:
            base_conf += 8
            reason += f" | RSI14={rsi14:.1f} сильный бычий (+8)"
        if hist_growing_up:
            base_conf += 5
            reason += " | Гистограмма растёт (+5)"

    elif sell_wins and sell_met >= _MIN_MET:
        direction = "SELL"
        conditions_met = sell_met
        base_conf = 60 + (sell_met - _MIN_MET) * 10
        reason = " | ".join(sell_parts)
        if rsi14 < 40:
            base_conf += 8
            reason += f" | RSI14={rsi14:.1f} сильный медвежий (+8)"
        if hist_growing_dn:
            base_conf += 5
            reason += " | Гистограмма растёт вниз (+5)"

    if direction == "NONE":
        return _none(reason, {
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds, "sell_conditions": sell_conds,
            "macd": round(macd_now, 6), "signal": round(sig_now, 6),
            "hist": round(hist_now, 6), "hist_prev": round(hist_prev, 6),
            "rsi14": round(rsi14, 1), "is_flickering": is_flickering,
        })

    return StrategyResult(
        direction=direction,
        confidence=max(0.0, min(100.0, base_conf)),
        conditions_met=conditions_met,
        total_conditions=_TOTAL,
        strategy_name="otc_trend_confirm",
        reasoning=reason,
        debug={
            "buy_met": buy_met, "sell_met": sell_met,
            "buy_conditions": buy_conds if direction == "BUY" else {},
            "sell_conditions": sell_conds if direction == "SELL" else {},
            "macd": round(macd_now, 6), "signal": round(sig_now, 6),
            "hist": round(hist_now, 6), "hist_prev": round(hist_prev, 6),
            "rsi14": round(rsi14, 1), "is_flickering": is_flickering,
            "expiry": "2m",
        }
    )


def _check_buy(hist_now, hist_prev, hist_cross_up, macd_now, sig_now,
               rsi14, is_flickering, hist_growing, macd_rsi_bull=50.0):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # C1: Histogram crossed from negative to positive
    conds["hist_cross_up"] = hist_cross_up
    if hist_cross_up:
        met += 1; parts.append(f"MACD гистограмма перешла в плюс ({hist_prev:.6f}→{hist_now:.6f})")

    # C2: MACD line above signal line
    c2 = macd_now > sig_now
    conds["macd_above_signal"] = c2
    if c2:
        met += 1; parts.append(f"MACD выше сигнальной ({macd_now:.6f}>{sig_now:.6f})")

    # C3: RSI above threshold (pair-adapted: 50 normal, 52 calm)
    c3 = rsi14 > macd_rsi_bull
    conds["rsi14_bullish"] = c3
    if c3:
        met += 1; parts.append(f"RSI(14)={rsi14:.1f} выше {macd_rsi_bull:.0f}")

    # C4: Not flickering (MACD not rapidly switching sides)
    c4 = not is_flickering
    conds["macd_stable"] = c4
    if c4:
        met += 1; parts.append("MACD стабильный (не мерцает)")

    # C5: RSI not in neutral zone — pair-adapted zone
    neutral_lo = max(45.0, macd_rsi_bull - 5.0)
    neutral_hi = min(55.0, macd_rsi_bull + 5.0)
    c5 = rsi14 < neutral_lo or rsi14 > neutral_hi
    conds["rsi14_decisive"] = c5
    if c5:
        met += 1; parts.append(f"RSI(14)={rsi14:.1f} вне нейтральной зоны {neutral_lo:.0f}-{neutral_hi:.0f}")

    return met, parts, conds


def _check_sell(hist_now, hist_prev, hist_cross_dn, macd_now, sig_now,
                rsi14, is_flickering, hist_growing, macd_rsi_bear=50.0):
    met = 0
    parts = []
    conds: dict[str, bool] = {}

    # C1: Histogram crossed from positive to negative
    conds["hist_cross_dn"] = hist_cross_dn
    if hist_cross_dn:
        met += 1; parts.append(f"MACD гистограмма перешла в минус ({hist_prev:.6f}→{hist_now:.6f})")

    # C2: MACD line below signal line
    c2 = macd_now < sig_now
    conds["macd_below_signal"] = c2
    if c2:
        met += 1; parts.append(f"MACD ниже сигнальной ({macd_now:.6f}<{sig_now:.6f})")

    # C3: RSI below threshold (pair-adapted: 50 normal, 48 calm)
    c3 = rsi14 < macd_rsi_bear
    conds["rsi14_bearish"] = c3
    if c3:
        met += 1; parts.append(f"RSI(14)={rsi14:.1f} ниже {macd_rsi_bear:.0f}")

    # C4: Not flickering
    c4 = not is_flickering
    conds["macd_stable"] = c4
    if c4:
        met += 1; parts.append("MACD стабильный (не мерцает)")

    # C5: RSI decisive — pair-adapted neutral zone
    neutral_lo = max(45.0, macd_rsi_bear - 5.0)
    neutral_hi = min(55.0, macd_rsi_bear + 5.0)
    c5 = rsi14 < neutral_lo or rsi14 > neutral_hi
    conds["rsi14_decisive"] = c5
    if c5:
        met += 1; parts.append(f"RSI(14)={rsi14:.1f} вне нейтральной зоны {neutral_lo:.0f}-{neutral_hi:.0f}")

    return met, parts, conds


def _rsi14(close_s: pd.Series) -> float:
    """RSI with period 14 (used for trend filter in this strategy)."""
    try:
        import pandas_ta as ta
        s = ta.rsi(close_s, length=14)
        v = float(s.iloc[-1]) if s is not None else 50.0
        return v if not np.isnan(v) else 50.0
    except Exception:
        period = 14
        delta = close_s.diff()
        gain  = delta.clip(lower=0)
        loss  = (-delta).clip(lower=0)
        ag    = gain.ewm(com=period - 1, min_periods=period).mean()
        al    = loss.ewm(com=period - 1, min_periods=period).mean()
        rs    = ag / al.replace(0, np.nan)
        r     = 100 - (100 / (1 + rs))
        v     = float(r.iloc[-1])
        return v if not np.isnan(v) else 50.0


def _none(reason: str, extra: dict | None = None) -> StrategyResult:
    return StrategyResult("NONE", 0.0, 0, _TOTAL, "otc_trend_confirm", reason, extra or {})
