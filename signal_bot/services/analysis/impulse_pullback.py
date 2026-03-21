"""
Impulse + Pullback Strategy
Looks for: strong impulse → weak pullback → continuation confirmation.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class ImpulsePullbackResult:
    direction: str        # buy | sell | none
    buy_score: float      # 0-100
    sell_score: float     # 0-100
    pattern_strength: float
    explanation: str


def impulse_pullback_strategy(df: pd.DataFrame) -> ImpulsePullbackResult:
    n = len(df)
    if n < 12:
        return ImpulsePullbackResult("none", 0.0, 0.0, 0.0, "Недостаточно данных")

    op  = df["open"].values
    cl  = df["close"].values
    hi  = df["high"].values
    lo  = df["low"].values

    body      = cl - op            # positive = bull, negative = bear
    body_abs  = np.abs(body)
    avg_body  = float(np.mean(body_abs[-20:])) if n >= 20 else float(np.mean(body_abs))
    if avg_body == 0:
        avg_body = 1e-8

    # ── Find impulse in recent 15 bars ────────────────────────────────────────
    # We scan backwards from bar -4 (need room for pullback + confirmation)
    scan_start = max(0, n - 15)
    best_bull = _find_pattern(body, body_abs, avg_body, "bull", scan_start, n)
    best_bear = _find_pattern(body, body_abs, avg_body, "bear", scan_start, n)

    if best_bull is None and best_bear is None:
        return ImpulsePullbackResult(
            "none", 0.0, 0.0, 0.0,
            "Нет чёткого импульс+откат паттерна"
        )

    buy_score  = 0.0
    sell_score = 0.0
    parts      = []

    if best_bull is not None:
        buy_score = best_bull
        parts.append(f"Бычий импульс→откат→продолжение (оценка {best_bull:.0f})")

    if best_bear is not None:
        sell_score = best_bear
        parts.append(f"Медвежий импульс→откат→продолжение (оценка {best_bear:.0f})")

    direction = "none"
    if buy_score > sell_score and buy_score >= 40:
        direction = "buy"
    elif sell_score > buy_score and sell_score >= 40:
        direction = "sell"

    pattern_strength = max(buy_score, sell_score)

    return ImpulsePullbackResult(
        direction=direction,
        buy_score=buy_score,
        sell_score=sell_score,
        pattern_strength=pattern_strength,
        explanation="; ".join(parts) if parts else "Нет паттерна",
    )


def _find_pattern(
    body: np.ndarray,
    body_abs: np.ndarray,
    avg_body: float,
    side: str,   # "bull" | "bear"
    start: int,
    n: int,
) -> float | None:
    """
    Returns a 0-100 score if a valid impulse+pullback+continuation is found.
    Returns None if not found.
    """
    sign = 1 if side == "bull" else -1  # bull impulse = positive body
    best: float | None = None

    # We need at least: impulse(3) + pullback(1) + confirm(1) = 5 bars
    for imp_start in range(start, n - 4):
        # ── Impulse: 3-5 consecutive candles in direction ─────────────────
        imp_count = 0
        imp_size  = 0.0
        for i in range(imp_start, min(imp_start + 5, n - 2)):
            if sign * body[i] > 0:
                imp_count += 1
                imp_size  += body_abs[i]
            else:
                break

        if imp_count < 3:
            continue

        imp_end  = imp_start + imp_count
        imp_avg  = imp_size / imp_count

        # Impulse must be meaningful (above average body size)
        if imp_avg < avg_body * 0.7:
            continue

        # ── Pullback: 1-2 candles in opposite direction, smaller ─────────
        pb_start = imp_end
        pb_count = 0
        pb_size  = 0.0
        for i in range(pb_start, min(pb_start + 2, n - 1)):
            if sign * body[i] < 0:
                pb_count += 1
                pb_size  += body_abs[i]
            else:
                break

        if pb_count < 1:
            continue

        pb_avg = pb_size / pb_count
        # Pullback must be weaker than impulse
        if pb_avg >= imp_avg * 0.85:
            continue

        # ── Confirmation: last candle resumes impulse direction ───────────
        conf_idx = pb_start + pb_count
        if conf_idx >= n:
            continue
        if sign * body[conf_idx] <= 0:
            continue

        # ── Score ─────────────────────────────────────────────────────────
        # Pattern quality = impulse strength × pullback weakness × recency
        impulse_quality  = min(1.0, imp_avg / (avg_body * 1.5))   # 0-1
        pullback_quality = 1.0 - (pb_avg / imp_avg)                # 0-1 (lower pb = higher)
        recency          = 1.0 - (n - conf_idx - 1) / max(1, n - start)  # more recent = better

        raw_score = (impulse_quality * 0.45 + pullback_quality * 0.30 + recency * 0.25) * 100
        score     = float(min(100.0, raw_score))

        if best is None or score > best:
            best = score

    return best
