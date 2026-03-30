"""
Pattern: Level Rejection
Price approaches a fresh S/R level, shows a rejection candle, then confirms.

BUY:  Price near support → long lower wick (rejection) → bullish confirmation
SELL: Price near resistance → long upper wick (rejection) → bearish confirmation

Score components (0-100):
  - Rejection candle quality  35%
  - Confirmation candle       30%
  - Level quality             20%
  - Room to opposite level    15%

fit_for: ["1m"] — fast reaction, needs tight confirmation
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from .levels_v2 import Level

_APPROACH_PCT  = 0.0015   # 0.15% — price within this of level = "approaching"
_MIN_WICK_RATIO = 1.5     # lower wick must be >= 1.5x body for rejection
_MIN_SCORE     = 40.0     # minimum to count as a valid pattern


@dataclass
class PatternResult:
    name: str
    direction: str          # "BUY" | "SELL" | "NONE"
    score: float            # 0-100
    fit_for: list[str]      # ["1m"] | ["2m"] | ["1m","2m"]
    pattern_quality: float  # 0-100
    level_quality: float    # 0-100
    rejection_reason: str
    debug: dict


def detect_level_rejection(
    df: pd.DataFrame,
    supports: list[Level],
    resistances: list[Level],
) -> PatternResult:
    _none = _make_none

    n   = len(df)
    if n < 4:
        return _none("n<4")

    cl  = df["close"].values
    op  = df["open"].values
    hi  = df["high"].values
    lo  = df["low"].values
    price = float(cl[-1])

    avg_body = float(np.mean(np.abs(cl[-min(15, n):] - op[-min(15, n):]))) or 1e-8

    best_buy  = _eval_rejection(cl, op, hi, lo, n, price, avg_body,
                                "BUY",  supports,    resistances)
    best_sell = _eval_rejection(cl, op, hi, lo, n, price, avg_body,
                                "SELL", resistances, supports)

    if best_buy["score"] <= _MIN_SCORE and best_sell["score"] <= _MIN_SCORE:
        reason = f"buy={best_buy['score']:.0f} sell={best_sell['score']:.0f} both below {_MIN_SCORE}"
        return _none(reason, buy_dbg=best_buy, sell_dbg=best_sell)

    if best_buy["score"] >= best_sell["score"] and best_buy["score"] > _MIN_SCORE:
        return PatternResult(
            name="level_rejection",
            direction="BUY",
            score=best_buy["score"],
            fit_for=["1m", "2m"],
            pattern_quality=best_buy["pattern_q"],
            level_quality=best_buy["level_q"],
            rejection_reason="",
            debug={"buy": best_buy, "sell": best_sell},
        )
    elif best_sell["score"] > _MIN_SCORE:
        return PatternResult(
            name="level_rejection",
            direction="SELL",
            score=best_sell["score"],
            fit_for=["1m", "2m"],
            pattern_quality=best_sell["pattern_q"],
            level_quality=best_sell["level_q"],
            rejection_reason="",
            debug={"buy": best_buy, "sell": best_sell},
        )

    return _none(f"scores too low: buy={best_buy['score']:.0f} sell={best_sell['score']:.0f}")


def _eval_rejection(
    cl, op, hi, lo, n, price, avg_body,
    direction: str,
    own_levels: list[Level],
    opp_levels: list[Level],
) -> dict:
    best = {"score": 0.0, "pattern_q": 0.0, "level_q": 0.0, "reason": "no level found"}
    is_buy = direction == "BUY"

    for lvl in own_levels[:6]:
        if lvl.touches < 1:
            continue
        if lvl.is_broken:
            continue

        lp = lvl.price

        # ── Check if price approached level in last 5 bars ───────────────────
        approach = False
        for i in range(1, min(6, n)):
            ref = float(lo[-i]) if is_buy else float(hi[-i])
            if abs(ref - lp) / lp <= _APPROACH_PCT:
                approach = True
                break
        if not approach:
            continue

        # ── Rejection candle (bar -2 or -1, not the confirmation bar) ────────
        rej_idx    = -2
        rej_body   = abs(float(cl[rej_idx]) - float(op[rej_idx]))
        rej_candle_q = 0.0

        if is_buy:
            wick = min(float(cl[rej_idx]), float(op[rej_idx])) - float(lo[rej_idx])
            # Long lower wick: wick >= 1.5x body, close above midpoint
            midpoint   = (float(hi[rej_idx]) + float(lo[rej_idx])) / 2
            close_up   = float(cl[rej_idx]) > midpoint
            wick_ratio = wick / (rej_body + 1e-10)
            if wick_ratio >= _MIN_WICK_RATIO and close_up:
                rej_candle_q = min(100.0, 40.0 + (wick_ratio - 1.5) * 15.0)
            elif wick_ratio >= 1.0 and close_up:
                rej_candle_q = 20.0
        else:
            wick = float(hi[rej_idx]) - max(float(cl[rej_idx]), float(op[rej_idx]))
            midpoint   = (float(hi[rej_idx]) + float(lo[rej_idx])) / 2
            close_dn   = float(cl[rej_idx]) < midpoint
            wick_ratio = wick / (rej_body + 1e-10)
            if wick_ratio >= _MIN_WICK_RATIO and close_dn:
                rej_candle_q = min(100.0, 40.0 + (wick_ratio - 1.5) * 15.0)
            elif wick_ratio >= 1.0 and close_dn:
                rej_candle_q = 20.0

        if rej_candle_q < 15.0:
            continue   # no meaningful rejection candle

        # ── Confirmation candle (most recent bar) ─────────────────────────────
        conf_body  = abs(float(cl[-1]) - float(op[-1]))
        conf_q     = 0.0

        if is_buy:
            # Bullish close, doesn't break rejection low
            conf_bullish = float(cl[-1]) > float(op[-1])
            no_break     = float(lo[-1]) >= float(lo[rej_idx]) * 0.9998
            if conf_bullish and no_break:
                conf_q = min(100.0, 40.0 + conf_body / avg_body * 15.0)
            elif conf_bullish:
                conf_q = 20.0
        else:
            conf_bearish = float(cl[-1]) < float(op[-1])
            no_break     = float(hi[-1]) <= float(hi[rej_idx]) * 1.0002
            if conf_bearish and no_break:
                conf_q = min(100.0, 40.0 + conf_body / avg_body * 15.0)
            elif conf_bearish:
                conf_q = 20.0

        if conf_q < 15.0:
            continue   # no meaningful confirmation

        # ── Level quality ─────────────────────────────────────────────────────
        level_q = min(100.0, (
            (30.0 if lvl.touches >= 2 else 10.0) +
            (20.0 if lvl.touches >= 3 else 0.0) +
            (20.0 if lvl.is_fresh else 0.0) +
            min(30.0, lvl.reaction_strength * 3000.0)
        ))

        # ── Room to opposite level ────────────────────────────────────────────
        room_pct  = 0.0
        if opp_levels:
            opp_p = opp_levels[0].price
            room_pct = abs(opp_p - price) / price * 100
        room_q = min(100.0, room_pct / 0.05 * 20.0)   # 0.05%→20pts, 0.25%→100pts

        if room_pct < 0.015:
            continue   # no room at all

        # ── Final score ───────────────────────────────────────────────────────
        pattern_q = rej_candle_q * 0.55 + conf_q * 0.45
        score     = pattern_q * 0.60 + level_q * 0.25 + room_q * 0.15

        if score > best["score"]:
            best = {
                "score":     round(score, 1),
                "pattern_q": round(pattern_q, 1),
                "level_q":   round(level_q, 1),
                "room_pct":  round(room_pct, 4),
                "level_price": lp,
                "touches":   lvl.touches,
                "is_fresh":  lvl.is_fresh,
                "rej_wick_ratio": round(wick_ratio, 2),
                "rej_q":     round(rej_candle_q, 1),
                "conf_q":    round(conf_q, 1),
                "reason":    f"level={lp:.5f}({lvl.touches}x) rej={rej_candle_q:.0f} conf={conf_q:.0f} room={room_pct:.3f}%",
            }

    return best


def _make_none(reason: str = "", buy_dbg: dict | None = None, sell_dbg: dict | None = None) -> PatternResult:
    return PatternResult(
        name="level_rejection",
        direction="NONE",
        score=0.0,
        fit_for=["1m", "2m"],
        pattern_quality=0.0,
        level_quality=0.0,
        rejection_reason=reason,
        debug={"buy": buy_dbg or {}, "sell": sell_dbg or {}, "reject": reason},
    )
