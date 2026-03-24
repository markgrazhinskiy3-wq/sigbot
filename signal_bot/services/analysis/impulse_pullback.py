"""
Impulse + Pullback Strategy — v4
Detects: dominant directional move → pullback → resumption candle.

Changes from v3:
  - Scan window: last 25 bars only (not 45) — stale patterns are irrelevant
  - Confirmation: bar must CLOSE in direction (not just "not strongly against")
  - Dominant-direction fallback: capped at 24 (below PARTIAL_MATCH_MIN=30),
    so it can only reinforce a real pattern, never create a standalone signal
  - Dominant-direction thresholds raised: 70% bars + last 6 bars 4/6+ + last bar in direction
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

    window = min(20, n)
    avg_body = float(np.mean(body_abs[-window:])) or 1e-8

    scan_start = max(0, n - 25)   # only last 25 bars
    best_bull, bull_detail = _find_pattern(body, body_abs, avg_body, "bull", scan_start, n)
    best_bear, bear_detail = _find_pattern(body, body_abs, avg_body, "bear", scan_start, n)

    # Dominant direction fallback — capped at 24 (below PARTIAL_MATCH_MIN=30)
    # Can boost a real but weak pattern; cannot create a standalone partial match
    if best_bull is None or best_bull < 20:
        dom_bull = _dominant_direction_score(body, body_abs, "bull", scan_start, n)
        if dom_bull > (best_bull or 0):
            best_bull = dom_bull
            bull_detail = {"fallback": "dominant_direction", "dom_score": round(dom_bull, 1)}
        else:
            best_bull = best_bull or 0
    if best_bear is None or best_bear < 20:
        dom_bear = _dominant_direction_score(body, body_abs, "bear", scan_start, n)
        if dom_bear > (best_bear or 0):
            best_bear = dom_bear
            bear_detail = {"fallback": "dominant_direction", "dom_score": round(dom_bear, 1)}
        else:
            best_bear = best_bear or 0

    buy_score  = float(best_bull or 0.0)
    sell_score = float(best_bear or 0.0)

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

    # Pick the winning side detail for debug
    winner_detail = (bull_detail if direction == "buy" else bear_detail) or {}

    debug: dict = {
        "bull_score":     round(buy_score, 1),
        "bear_score":     round(sell_score, 1),
        "avg_body":       round(avg_body, 6),
        "scan_bars":      n - scan_start,
        # Pattern structure detail (populated when a full impulse+pb+confirm found)
        "imp_bars":       winner_detail.get("imp_bars"),
        "pb_bars":        winner_detail.get("pb_bars"),
        "retracement_pct": winner_detail.get("retracement_pct"),
        "conf_bar_idx":   winner_detail.get("conf_bar_idx"),
        "bars_ago":       winner_detail.get("bars_ago"),
        # Score breakdown
        "impulse_q":      winner_detail.get("impulse_q"),
        "pullback_q":     winner_detail.get("pullback_q"),
        "recency":        winner_detail.get("recency"),
        "conf_bonus":     winner_detail.get("conf_bonus"),
        "score_formula":  winner_detail.get("score_formula"),
    }
    # Remove None entries for cleaner output
    debug = {k: v for k, v in debug.items() if v is not None}

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
) -> tuple[float | None, dict | None]:
    """
    Impulse + pullback + confirmation scan over last 25 bars.
    Confirmation bar MUST close in the signal direction.
    Returns (best_score, detail_dict) or (None, None).
    detail_dict contains full breakdown: imp_bars, pb_bars, retracement_pct,
    conf_bar_idx, bars_ago, impulse_q, pullback_q, recency, conf_bonus, score_formula.
    """
    sign = 1 if side == "bull" else -1
    best_score: float | None = None
    best_detail: dict | None = None

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

        # Impulse must be meaningful (>= 60% of avg body)
        if imp_avg < avg_body * 0.6:
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

        # Pullback must be weaker than impulse
        if pb_avg >= imp_avg * 0.95:
            continue

        # ── Confirmation: bar must CLOSE in signal direction ─────────────
        conf_idx = pb_start + pb_count
        if conf_idx >= n:
            continue

        conf_body = body[conf_idx]
        if sign * conf_body <= 0:   # must close in direction — no doji/neutral allowed
            continue

        # ── Score ─────────────────────────────────────────────────────────
        impulse_q  = min(1.0, imp_avg / (avg_body * 1.2))
        pullback_q = max(0.0, 1.0 - (pb_avg / imp_avg))
        recency    = 1.0 - (n - conf_idx - 1) / max(1, n - start)
        conf_bonus = 0.15 if sign * conf_body > avg_body * 0.3 else 0.0

        raw   = (impulse_q * 0.40 + pullback_q * 0.25 + recency * 0.25 + conf_bonus) * 100
        score = float(min(100.0, max(0.0, raw)))

        if best_score is None or score > best_score:
            best_score = score
            retracement_pct = round(pb_avg / imp_avg * 100, 1)
            bars_ago        = n - conf_idx - 1
            best_detail = {
                "imp_bars":        imp_count,
                "pb_bars":         pb_count,
                "retracement_pct": retracement_pct,
                "conf_bar_idx":    int(conf_idx),
                "bars_ago":        int(bars_ago),
                "impulse_q":       round(impulse_q,  3),
                "pullback_q":      round(pullback_q, 3),
                "recency":         round(recency,    3),
                "conf_bonus":      round(conf_bonus, 3),
                "score_formula":   (
                    f"({impulse_q:.2f}×0.40 + {pullback_q:.2f}×0.25 "
                    f"+ {recency:.2f}×0.25 + {conf_bonus:.2f})×100 = {score:.1f}"
                ),
            }

    return best_score, best_detail


def _dominant_direction_score(
    body: np.ndarray,
    body_abs: np.ndarray,
    side: str,
    start: int,
    n: int,
) -> float:
    """
    Fallback: checks if recent bars are strongly and consistently in direction.
    Capped at 24 — cannot create a partial match on its own (PARTIAL_MATCH_MIN=30).
    Requires: 70%+ of bars in direction, last 6 bars 4/6+ in direction, last bar in direction.
    """
    sign = 1 if side == "bull" else -1
    recent = body[start:]
    if len(recent) < 6:
        return 0.0

    in_dir = np.sum(sign * recent > 0)
    total  = len(recent)
    ratio  = in_dir / total

    if ratio < 0.70:
        return 0.0

    # Last 6 bars: need 4+ in direction
    last6 = body[-6:]
    last6_dir = int(np.sum(sign * last6 > 0))
    if last6_dir < 4:
        return 0.0

    # Last bar must be in direction
    if sign * body[-1] <= 0:
        return 0.0

    dominance = (ratio - 0.70) / 0.30   # 0 at 70%, 1 at 100%
    return float(min(24.0, dominance * 20.0 + (last6_dir / 6) * 10.0))
