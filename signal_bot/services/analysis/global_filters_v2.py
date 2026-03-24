"""
Global Filters V2 — Applied AFTER pattern detection.

Hard filters (any failure → reject signal):
  1. dead_market   — avg body too small, market is noise
  2. no_room       — opposite level too close
  3. exhaustion    — too many consecutive candles in one direction

Soft filters (reduce score but don't block):
  4. noisy_structure — choppy price action reduces confidence

no_room thresholds by pattern:
  - level_rejection / false_breakout  : 0.015%  (default)
  - impulse_pullback                  : skipped  (_compute_levels treats recent swing high
                                                  as resistance — but that IS the target;
                                                  quality enforced via direction_gap + RANGE
                                                  penalty + countertrend rejection in engine)
  - compression_breakout              : skipped  (breaks through the range boundary)
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
    score_penalty: float = 0.0
    reasons: list[str]   = field(default_factory=list)


_DEAD_BODY_PCT    = 0.003   # avg body < 0.003% of price = dead
_NO_ROOM_PCT      = 0.015   # opposite level < 0.015% away = no room (level_rejection / false_breakout)
_EXHAUST_BARS     = 7       # max consecutive same-direction bars
_NOISY_THRESHOLD  = 0.55    # if direction flip rate > this = noisy


def apply_global_filters(
    df: pd.DataFrame,
    direction: str,
    supports: list,
    resistances: list,
    pattern_name: str = "",
    expiry: str = "1m",
) -> FilterResult:
    """
    Apply all global filters.

    Args:
        df:           15s OHLC DataFrame
        direction:    "BUY" | "SELL"
        supports:     list of Level objects (supports)
        resistances:  list of Level objects (resistances)
        pattern_name: pattern being evaluated (affects filter thresholds)
        expiry:       user-selected trade expiry ("1m" | "2m")

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
    # compression_breakout: breaks through range boundary by design — skip
    # impulse_pullback 2m:  continuation expects to punch through — skip
    # impulse_pullback 1m:  still applies but softer threshold (0.010%)
    # all others:           standard threshold (0.015%)
    if pattern_name in ("compression_breakout", "impulse_pullback"):
        # impulse_pullback: _compute_levels treats the recent swing HIGH as resistance,
        # which is the CONTINUATION TARGET for a BUY — not a blocker.
        # Quality gates (direction_gap >= 13, RANGE penalty, countertrend reject)
        # handle impulse_pullback signal quality inside the engine.
        # compression_breakout: breaks through range boundary by design.
        skip_no_room = True
        no_room_threshold = _NO_ROOM_PCT
    else:
        skip_no_room = False
        no_room_threshold = _NO_ROOM_PCT

    no_room = False
    if not skip_no_room:
        if direction == "BUY" and resistances:
            opp_price = resistances[0].price
            room_pct  = (opp_price - price) / price * 100
            if room_pct < no_room_threshold:
                no_room = True
                reasons.append(
                    f"no_room_BUY: resistance {opp_price:.5f} only {room_pct:.4f}% away "
                    f"(threshold={no_room_threshold}%)"
                )
        elif direction == "SELL" and supports:
            opp_price = supports[0].price
            room_pct  = (price - opp_price) / price * 100
            if room_pct < no_room_threshold:
                no_room = True
                reasons.append(
                    f"no_room_SELL: support {opp_price:.5f} only {room_pct:.4f}% away "
                    f"(threshold={no_room_threshold}%)"
                )

    if no_room:
        return FilterResult(passed=False, no_room=True, reasons=reasons)

    # ── 3. Exhaustion filter (consecutive streak check) ───────────────────────
    # Counts CONSECUTIVE same-direction candles from the current bar backwards.
    # Only a persistent unbroken streak signals exhaustion.
    # compression_breakout naturally has a pre-entry streak — skip.
    # impulse_pullback has a post-impulse pullback which disrupts the streak — ok to check.
    exhausted    = False
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
