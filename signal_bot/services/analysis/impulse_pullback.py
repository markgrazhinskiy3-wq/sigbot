"""
Impulse + Pullback Strategy — v5.1

Changes from v5:
  - conf_body_ratio added to debug (abs(conf_body)/avg_body)
    Used by engine to filter borderline 1m setups with weak confirmation.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass

# ── Thresholds ─────────────────────────────────────────────────────────────────
_MIN_IMPULSE_RATIO = 1.0     # impulse avg body must be >= 1.0× avg_body
_MAX_PB_RATIO      = 0.55    # pullback avg body must be < 55% of impulse
_MIN_CONF_RATIO    = 0.35    # confirmation body must be >= 0.35× avg_body
_MIN_SCORE         = 35.0    # pattern score threshold (was 25)
_MIN_DIRECTION_GAP = 13.0    # winner must beat loser by at least 13 pts
_COUNTERTREND_PCT  = 0.60    # if 60%+ of last-15 bars are against direction → countertrend


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

    # Dominant direction fallback — capped at 24 (below _MIN_SCORE=35)
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
    direction_gap = round(abs(buy_score - sell_score), 1)

    direction = "none"
    if buy_score >= sell_score and buy_score >= _MIN_SCORE:
        if (buy_score - sell_score) >= _MIN_DIRECTION_GAP:
            direction = "buy"
    elif sell_score > buy_score and sell_score >= _MIN_SCORE:
        if (sell_score - buy_score) >= _MIN_DIRECTION_GAP:
            direction = "sell"

    # ── Countertrend detection ─────────────────────────────────────────────────
    countertrend = False
    if direction != "none" and n >= 15:
        sign = 1 if direction == "buy" else -1
        last15 = body[-15:]
        against = sum(1 for b in last15 if sign * b < 0)
        against_ratio = against / len(last15)
        if against_ratio > _COUNTERTREND_PCT:
            countertrend = True

    parts = []
    if buy_score >= _MIN_SCORE:
        parts.append(f"Бычий паттерн (score {buy_score:.0f})")
    if sell_score >= _MIN_SCORE:
        parts.append(f"Медвежий паттерн (score {sell_score:.0f})")

    winner_detail = (bull_detail if direction == "buy" else bear_detail) or {}

    debug: dict = {
        "bull_score":      round(buy_score, 1),
        "bear_score":      round(sell_score, 1),
        "direction_gap":   direction_gap,
        "avg_body":        round(avg_body, 6),
        "scan_bars":       n - scan_start,
        "countertrend":    countertrend,
        # Pattern structure detail
        "imp_bars":        winner_detail.get("imp_bars"),
        "pb_bars":         winner_detail.get("pb_bars"),
        "retracement_pct": winner_detail.get("retracement_pct"),
        "conf_bar_idx":    winner_detail.get("conf_bar_idx"),
        "bars_ago":        winner_detail.get("bars_ago"),
        "conf_body_ratio": winner_detail.get("conf_body_ratio"),   # v5.1: for borderline filter
        # Score breakdown
        "impulse_q":       winner_detail.get("impulse_q"),
        "pullback_q":      winner_detail.get("pullback_q"),
        "recency":         winner_detail.get("recency"),
        "conf_bonus":      winner_detail.get("conf_bonus"),
        "score_formula":   winner_detail.get("score_formula"),
    }
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

    Requirements:
      - Impulse avg body >= 1.0× avg_body
      - Pullback avg body < 55% of impulse
      - Confirmation body >= 0.35× avg_body
      - Confirmation must CLOSE in signal direction

    Returns (best_score, detail_dict) or (None, None).
    """
    sign = 1 if side == "bull" else -1
    best_score: float | None = None
    best_detail: dict | None = None

    for imp_start in range(start, n - 3):
        # ── Impulse: 2-6 consecutive bars in direction ────────────────────
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

        if imp_avg < avg_body * _MIN_IMPULSE_RATIO:
            continue

        # ── Pullback: 1-3 bars against direction ──────────────────────────
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

        if pb_avg >= imp_avg * _MAX_PB_RATIO:
            continue

        # ── Confirmation: must close in direction AND be a real body ──────
        conf_idx = pb_start + pb_count
        if conf_idx >= n:
            continue

        conf_body = body[conf_idx]
        if sign * conf_body <= 0:
            continue
        if abs(conf_body) < avg_body * _MIN_CONF_RATIO:
            continue

        # ── Score ──────────────────────────────────────────────────────────
        impulse_q  = min(1.0, imp_avg / (avg_body * 1.2))
        pullback_q = max(0.0, 1.0 - (pb_avg / (imp_avg * _MAX_PB_RATIO)))
        recency    = 1.0 - (n - conf_idx - 1) / max(1, n - start)
        conf_bonus = 0.15 if abs(conf_body) > avg_body * 0.6 else 0.0

        raw   = (impulse_q * 0.40 + pullback_q * 0.25 + recency * 0.25 + conf_bonus) * 100
        score = float(min(100.0, max(0.0, raw)))

        if best_score is None or score > best_score:
            best_score = score
            retracement_pct  = round(pb_avg / imp_avg * 100, 1)
            bars_ago         = n - conf_idx - 1
            conf_body_ratio  = round(abs(conf_body) / avg_body, 3)   # v5.1
            best_detail = {
                "imp_bars":        imp_count,
                "pb_bars":         pb_count,
                "retracement_pct": retracement_pct,
                "conf_bar_idx":    int(conf_idx),
                "bars_ago":        int(bars_ago),
                "conf_body_ratio": conf_body_ratio,                   # v5.1
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
    Capped at 24 — below _MIN_SCORE=35, so cannot create a standalone signal.
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

    last6 = body[-6:]
    last6_dir = int(np.sum(sign * last6 > 0))
    if last6_dir < 4:
        return 0.0

    if sign * body[-1] <= 0:
        return 0.0

    dominance = (ratio - 0.70) / 0.30
    return float(min(24.0, dominance * 20.0 + (last6_dir / 6) * 10.0))
