"""
Global Filters V2 — Applied AFTER pattern detection.

Hard filters (any failure → reject signal):
  1. dead_market   — avg body too small, market is noise
  2. no_room       — opposite level too close
  3. exhaustion    — too many consecutive candles in one direction

Soft filters (reduce score but don't block):
  4. noisy_structure — choppy price action reduces confidence
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass, field


@dataclass
class FilterResult:
    passed: bool
    dead_market: bool    = False
    no_room: bool        = False
    exhausted: bool      = False
    noisy: bool          = False
    score_penalty: float = 0.0     # soft penalty to apply to pattern score
    reasons: list[str]   = field(default_factory=list)


_DEAD_BODY_PCT    = 0.003   # avg body < 0.003% of price = dead
_NO_ROOM_PCT      = 0.015   # opposite level < 0.015% away = no room
_EXHAUST_BARS     = 7       # max consecutive same-direction bars
_NOISY_THRESHOLD  = 0.55    # if direction flip rate > this = noisy


def apply_global_filters(
    df: pd.DataFrame,
    direction: str,
    supports: list,
    resistances: list,
    pattern_name: str = "",
) -> FilterResult:
    """
    Apply all global filters.

    Args:
        df:           15s OHLC DataFrame
        direction:    "BUY" | "SELL"
        supports:     list of Level objects (supports)
        resistances:  list of Level objects (resistances)
        pattern_name: pattern being evaluated (affects filter thresholds)

    Returns:
        FilterResult — passed=False means reject signal
    """
    n       = len(df)
    cl      = df["close"].values
    op      = df["open"].values
    price   = float(cl[-1])
    reasons: list[str] = []
    penalty = 0.0

    # ── 1. Dead market filter ─────────────────────────────────────────────────
    lookback     = min(20, n)
    avg_body_pct = float(np.mean(np.abs(cl[-lookback:] - op[-lookback:]))) / price * 100 if price > 0 else 0.0
    dead_market  = avg_body_pct < _DEAD_BODY_PCT
    if dead_market:
        reasons.append(f"dead_market: avg_body={avg_body_pct:.5f}% < {_DEAD_BODY_PCT}%")
        return FilterResult(passed=False, dead_market=True, reasons=reasons)

    # ── 2. No room filter ─────────────────────────────────────────────────────
    # impulse_pullback is a CONTINUATION pattern — it expects to break through
    # the nearest resistance/support; don't apply no_room block for it.
    # compression_breakout also breaks through the range boundary — same logic.
    skip_no_room = pattern_name in ("impulse_pullback", "compression_breakout")

    no_room = False
    if not skip_no_room:
        if direction == "BUY" and resistances:
            opp_price = resistances[0].price
            room_pct  = (opp_price - price) / price * 100
            if room_pct < _NO_ROOM_PCT:
                no_room = True
                reasons.append(f"no_room_BUY: resistance {opp_price:.5f} only {room_pct:.4f}% away")
        elif direction == "SELL" and supports:
            opp_price = supports[0].price
            room_pct  = (price - opp_price) / price * 100
            if room_pct < _NO_ROOM_PCT:
                no_room = True
                reasons.append(f"no_room_SELL: support {opp_price:.5f} only {room_pct:.4f}% away")

    if no_room:
        return FilterResult(passed=False, no_room=True, reasons=reasons)

    # ── 3. Exhaustion filter (consecutive streak check) ───────────────────────
    # Counts CONSECUTIVE same-direction candles from the current bar backwards.
    # Only a persistent unbroken streak signals exhaustion.
    # compression_breakout and impulse_pullback naturally have pre-entry streaks — skip.
    exhausted = False
    skip_exhaust = pattern_name in ("compression_breakout",)

    if not skip_exhaust:
        check_n = min(_EXHAUST_BARS, n)
        if direction == "BUY":
            bull_streak = 0
            for i in range(1, check_n + 1):
                if float(cl[-i]) > float(op[-i]):
                    bull_streak += 1
                else:
                    break
            if bull_streak >= _EXHAUST_BARS:
                exhausted = True
                reasons.append(f"exhaustion_BUY: {bull_streak} consecutive bull bars — overbought")
        else:
            bear_streak = 0
            for i in range(1, check_n + 1):
                if float(cl[-i]) < float(op[-i]):
                    bear_streak += 1
                else:
                    break
            if bear_streak >= _EXHAUST_BARS:
                exhausted = True
                reasons.append(f"exhaustion_SELL: {bear_streak} consecutive bear bars — oversold")

    if exhausted:
        return FilterResult(passed=False, exhausted=True, reasons=reasons)

    # ── 4. Noisy structure (soft — penalty only) ──────────────────────────────
    noisy = False
    if n >= 8:
        recent  = min(10, n)
        dirs    = [1 if float(cl[-i]) > float(op[-i]) else -1 for i in range(1, recent + 1)]
        flips   = sum(1 for j in range(1, len(dirs)) if dirs[j] != dirs[j - 1])
        flip_rt = flips / max(1, len(dirs) - 1)
        if flip_rt > _NOISY_THRESHOLD:
            noisy   = True
            penalty = min(15.0, (flip_rt - _NOISY_THRESHOLD) * 50.0)
            reasons.append(f"noisy: flip_rate={flip_rt:.2f} penalty=-{penalty:.0f}")

    return FilterResult(
        passed=True,
        dead_market=False,
        no_room=False,
        exhausted=exhausted,
        noisy=noisy,
        score_penalty=round(penalty, 1),
        reasons=reasons,
    )
