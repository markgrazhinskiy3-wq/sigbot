"""
Impulse + Pullback Strategy — relaxed v3
Detects: dominant directional move → any pullback → resumption.

Changes from v2:
  - Requires only 2+ bars in direction (not 3-5 strict consecutive)
  - Pullback allowed to be up to 80% of impulse size (was 85%)
  - Confirmation: any bar resuming direction (even tiny)
  - Partial match: returns score > 0 even for weak/incomplete patterns
  - Scan window: last 45 bars
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class ImpulsePullbackResult:
    direction: str
    buy_score: float
    sell_score: float
    pattern_strength: float
    explanation: str
    debug: dict


def impulse_pullback_strategy(df: pd.DataFrame) -> ImpulsePullbackResult:
    n = len(df)
    if n < 8:
        return ImpulsePullbackResult(
            "none", 0.0, 0.0, 0.0,
            "Недостаточно данных",
            {"reason": "too_few_candles", "n": n}
        )

    op = df["open"].values
    cl = df["close"].values
    body = cl - op
    body_abs = np.abs(body)

    # Use recent average body (last 20 bars or all)
    window = min(20, n)
    avg_body = float(np.mean(body_abs[-window:])) or 1e-8

    scan_start = max(0, n - 45)
    best_bull = _find_pattern(body, body_abs, avg_body, "bull", scan_start, n)
    best_bear = _find_pattern(body, body_abs, avg_body, "bear", scan_start, n)

    # Also run a simple "dominant direction" check as fallback
    if best_bull is None or best_bull < 20:
        dom_bull = _dominant_direction_score(body, body_abs, "bull", scan_start, n)
        best_bull = max(best_bull or 0, dom_bull)
    if best_bear is None or best_bear < 20:
        dom_bear = _dominant_direction_score(body, body_abs, "bear", scan_start, n)
        best_bear = max(best_bear or 0, dom_bear)

    buy_score  = best_bull or 0.0
    sell_score = best_bear or 0.0

    direction = "none"
    if buy_score >= sell_score and buy_score >= 25:
        direction = "buy"
    elif sell_score > buy_score and sell_score >= 25:
        direction = "sell"

    parts = []
    if buy_score >= 25:
        parts.append(f"Бычий паттерн (score {buy_score:.0f})")
    if sell_score >= 25:
        parts.append(f"Медвежий паттерн (score {sell_score:.0f})")

    debug = {
        "bull_score": round(buy_score, 1),
        "bear_score": round(sell_score, 1),
        "avg_body": round(avg_body, 6),
        "scan_bars": n - scan_start,
    }

    return ImpulsePullbackResult(
        direction=direction,
        buy_score=buy_score,
        sell_score=sell_score,
        pattern_strength=max(buy_score, sell_score),
        explanation="; ".join(parts) if parts else "Паттерн не обнаружен",
        debug=debug,
    )


def _find_pattern(
    body: np.ndarray,
    body_abs: np.ndarray,
    avg_body: float,
    side: str,
    start: int,
    n: int,
) -> float | None:
    """
    Relaxed impulse+pullback+confirmation scan.
    Returns best score (0-100) or None.
    """
    sign = 1 if side == "bull" else -1
    best: float | None = None

    # Need at least: impulse(2) + pullback(1) + confirm(1) = 4 bars
    for imp_start in range(start, n - 3):
        # ── Impulse: 2-6 bars in direction ───────────────────────────────
        imp_count = 0
        imp_size  = 0.0
        for i in range(imp_start, min(imp_start + 6, n - 2)):
            if sign * body[i] > 0:
                imp_count += 1
                imp_size  += body_abs[i]
            else:
                break

        if imp_count < 2:
            continue

        imp_end = imp_start + imp_count
        imp_avg = imp_size / imp_count

        # Impulse must be meaningful (>= 50% of avg body — very relaxed)
        if imp_avg < avg_body * 0.5:
            continue

        # ── Pullback: 1-3 bars in opposite direction ─────────────────────
        pb_start = imp_end
        pb_count = 0
        pb_size  = 0.0
        for i in range(pb_start, min(pb_start + 3, n - 1)):
            if sign * body[i] < 0:
                pb_count += 1
                pb_size  += body_abs[i]
            else:
                break

        if pb_count < 1:
            continue

        pb_avg = pb_size / pb_count

        # Pullback must be weaker than impulse (< 100% — very relaxed, was 85%)
        if pb_avg >= imp_avg * 1.0:
            continue

        # ── Confirmation: any bar resuming direction ─────────────────────
        conf_idx = pb_start + pb_count
        if conf_idx >= n:
            continue

        # Confirmation: close in direction OR half-size body in direction
        conf_body = body[conf_idx]
        confirmed = sign * conf_body > 0 or sign * conf_body > -avg_body * 0.3

        if not confirmed:
            continue

        # ── Score ─────────────────────────────────────────────────────────
        impulse_q   = min(1.0, imp_avg / (avg_body * 1.2))   # how strong impulse
        pullback_q  = max(0.0, 1.0 - (pb_avg / imp_avg))     # how weak pullback
        recency     = 1.0 - (n - conf_idx - 1) / max(1, n - start)  # recent = better
        conf_bonus  = 0.15 if sign * conf_body > 0 else 0.0

        raw = (impulse_q * 0.40 + pullback_q * 0.25 + recency * 0.25 + conf_bonus) * 100
        score = float(min(100.0, max(0.0, raw)))

        if best is None or score > best:
            best = score

    return best


def _dominant_direction_score(
    body: np.ndarray,
    body_abs: np.ndarray,
    side: str,
    start: int,
    n: int,
) -> float:
    """
    Fallback: checks if recent bars are predominantly in direction.
    Returns partial score 0-45.
    """
    sign = 1 if side == "bull" else -1
    recent = body[start:]
    if len(recent) < 4:
        return 0.0

    in_dir  = np.sum(sign * recent > 0)
    total   = len(recent)
    ratio   = in_dir / total

    if ratio < 0.55:
        return 0.0

    # Check recent 4 bars bias
    last4 = body[-4:]
    last4_dir = np.sum(sign * last4 > 0)
    last4_ratio = last4_dir / 4

    if last4_ratio < 0.5:
        return 0.0

    # Score based on dominance strength
    dominance = (ratio - 0.55) / 0.45   # 0 at 55%, 1 at 100%
    return float(min(45.0, dominance * 45.0 + last4_ratio * 15.0))
