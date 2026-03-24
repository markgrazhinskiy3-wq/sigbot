"""
Global Filters V2 — Applied AFTER pattern detection.

Hard filters (any failure → reject signal):
  1. dead_market   — avg body too small, market is noise
  2. no_room       — opposite level too close
  3. exhaustion    — too many consecutive candles in one direction

Soft filters (reduce score but don't block):
  4. noisy_structure — choppy price action reduces confidence

no_room thresholds by pattern:
  - level_rejection                   : 0.015%  (handled internally in LR pattern)
  - false_breakout                    : SKIPPED  (pattern IS at the level; recovery is the check)
  - compression_breakout              : SKIPPED  (breaks through range boundary)
  - impulse_pullback                  : smart check (1m only, 2m skipped):
      * internal swing high/low (1-touch, fresh, recent) → skip (it's the target)
      * strong external level (>=2 touches, not broken)  → hard reject at 0.030%
      * ambiguous level (1-touch, older or broken)       → soft penalty up to 10pts
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


_DEAD_BODY_PCT     = 0.003   # avg body < 0.003% of price = dead
_NO_ROOM_PCT       = 0.015   # default: opposite level < 0.015% away = no room
_NO_ROOM_IP_1M_PCT = 0.030   # impulse_pullback 1m: external level within 0.030% → hard reject (was 0.025)
_IP_INTERNAL_IDX   = 12      # last_touch_idx <= 12 = likely the impulse's own bar swing
_EXHAUST_BARS      = 7       # max consecutive same-direction bars
_NOISY_THRESHOLD   = 0.55    # if direction flip rate > this = noisy

# Patterns that skip no_room entirely (they operate at/through levels)
_SKIP_NO_ROOM_PATTERNS = frozenset({"compression_breakout", "false_breakout"})


def _ip_no_room_check(
    direction: str,
    supports: list,
    resistances: list,
    price: float,
    expiry: str,
) -> tuple[str, str, float]:
    """
    Smart no_room check for impulse_pullback.

    Distinguishes between:
      - Internal impulse swing (1-touch, very fresh, recent bar):
            skip — it's the continuation target, not an external barrier
      - Strong external level (touches >= 2, not broken):
            hard reject at _NO_ROOM_IP_1M_PCT (0.020%)
      - Ambiguous level (1-touch but older, or broken):
            soft penalty proportional to closeness

    Only applies for 1m expiry. 2m continuation skips entirely.

    Returns: (action, reason_str, penalty_pts)
      action: "skip" | "hard" | "soft"
    """
    if expiry == "2m":
        return "skip", "", 0.0

    # Find the NEAREST level in the opposing direction
    if direction == "BUY":
        candidates = [
            (lvl, (lvl.price - price) / price * 100)
            for lvl in resistances
            if lvl.price > price
        ]
    else:
        candidates = [
            (lvl, (price - lvl.price) / price * 100)
            for lvl in supports
            if lvl.price < price
        ]

    if not candidates:
        return "skip", "", 0.0

    candidates.sort(key=lambda x: x[1])   # nearest first
    nearest, room_pct = candidates[0]

    if room_pct >= _NO_ROOM_IP_1M_PCT:
        return "skip", "", 0.0   # enough room, no issue

    # ── Classify the nearest level ──────────────────────────────────────────
    is_internal = (
        nearest.touches == 1
        and nearest.is_fresh
        and nearest.last_touch_idx <= _IP_INTERNAL_IDX
    )
    if is_internal:
        return (
            "skip",
            f"ip_internal_swing: {nearest.price:.5f} touches=1 "
            f"last_touch={nearest.last_touch_idx}bars — impulse's own high/low, not a barrier",
            0.0,
        )

    # Strong external level — hard reject
    if nearest.touches >= 2 and not nearest.is_broken:
        reason = (
            f"ip_no_room_{direction}: strong external level {nearest.price:.5f} "
            f"({nearest.touches}x touched, fresh={nearest.is_fresh}, "
            f"broken={nearest.is_broken}) "
            f"only {room_pct:.4f}% away (threshold {_NO_ROOM_IP_1M_PCT}%)"
        )
        return "hard", reason, 0.0

    # Ambiguous — soft penalty
    closeness = max(0.0, 1.0 - room_pct / _NO_ROOM_IP_1M_PCT)
    soft_penalty = round(min(10.0, closeness * 12.0), 1)
    reason = (
        f"ip_close_level_{direction}: ambiguous {nearest.price:.5f} "
        f"({nearest.touches}x, fresh={nearest.is_fresh}, broken={nearest.is_broken}) "
        f"{room_pct:.4f}% away → soft -{soft_penalty:.0f}pts"
    )
    return "soft", reason, soft_penalty


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
    avg_body_pct = (
        float(np.mean(np.abs(cl[-lookback:] - op[-lookback:]))) / price * 100
        if price > 0 else 0.0
    )
    dead_market = avg_body_pct < _DEAD_BODY_PCT
    if dead_market:
        reasons.append(f"dead_market: avg_body={avg_body_pct:.5f}% < {_DEAD_BODY_PCT}%")
        return FilterResult(passed=False, dead_market=True, reasons=reasons)

    # ── 2. No room filter ─────────────────────────────────────────────────────
    no_room = False

    if pattern_name in _SKIP_NO_ROOM_PATTERNS:
        # false_breakout: pattern IS at the level — recovery is the quality signal
        # compression_breakout: breaks through range boundary
        skip_no_room = True

    elif pattern_name == "impulse_pullback":
        # Smart check: distinguishes internal impulse swings from external levels
        kind, ip_reason, ip_penalty = _ip_no_room_check(
            direction, supports, resistances, price, expiry
        )
        if kind == "hard":
            reasons.append(ip_reason)
            return FilterResult(passed=False, no_room=True, reasons=reasons)
        elif kind == "soft":
            reasons.append(ip_reason)
            penalty += ip_penalty
        skip_no_room = True   # standard block already handled above

    else:
        skip_no_room = False

    if not skip_no_room:
        if direction == "BUY" and resistances:
            opp_price = resistances[0].price
            room_pct  = (opp_price - price) / price * 100
            if 0 < room_pct < _NO_ROOM_PCT:
                no_room = True
                reasons.append(
                    f"no_room_BUY: resistance {opp_price:.5f} only {room_pct:.4f}% away "
                    f"(threshold={_NO_ROOM_PCT}%)"
                )
        elif direction == "SELL" and supports:
            opp_price = supports[0].price
            room_pct  = (price - opp_price) / price * 100
            if 0 < room_pct < _NO_ROOM_PCT:
                no_room = True
                reasons.append(
                    f"no_room_SELL: support {opp_price:.5f} only {room_pct:.4f}% away "
                    f"(threshold={_NO_ROOM_PCT}%)"
                )

    if no_room:
        return FilterResult(passed=False, no_room=True, reasons=reasons)

    # ── 3. Exhaustion filter ───────────────────────────────────────────────────
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
