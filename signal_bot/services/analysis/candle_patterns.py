"""
Candle Pattern Detection — pin bar, engulfing, hammer.
All functions operate on raw OHLC arrays (last N bars).
"""
from __future__ import annotations
import numpy as np
from dataclasses import dataclass


@dataclass
class CandlePatternResult:
    pattern: str        # "pin_bar" | "engulfing" | "hammer" | "none"
    direction: str      # "bull" | "bear" | "none"
    quality: float      # 0.0 – 1.0 (how clean the pattern is)
    explanation: str


def detect_reversal_pattern(
    open_: np.ndarray,
    high:  np.ndarray,
    low:   np.ndarray,
    close: np.ndarray,
    avg_body: float,
    side: str,         # "bull" (bounce from support) or "bear" (bounce from resistance)
) -> CandlePatternResult:
    """
    Detect the best reversal pattern in the last 2 bars.
    `side` tells us what direction we expect the reversal to go.
    """
    results = []

    pb = _pin_bar(open_, high, low, close, avg_body, side)
    if pb.pattern != "none":
        results.append(pb)

    eg = _engulfing(open_, high, low, close, avg_body, side)
    if eg.pattern != "none":
        results.append(eg)

    hm = _hammer(open_, high, low, close, avg_body, side)
    if hm.pattern != "none":
        results.append(hm)

    if not results:
        return CandlePatternResult("none", "none", 0.0, "Паттерн не обнаружен")

    return max(results, key=lambda r: r.quality)


def _pin_bar(
    op: np.ndarray, hi: np.ndarray, lo: np.ndarray, cl: np.ndarray,
    avg_body: float, side: str,
) -> CandlePatternResult:
    """
    Pin bar: shadow toward level > 2× body.
    For bull side: long lower shadow.
    For bear side: long upper shadow.
    """
    body     = abs(cl[-1] - op[-1])
    total    = hi[-1] - lo[-1]
    if total == 0:
        return CandlePatternResult("none", "none", 0.0, "")

    if side == "bull":
        shadow = min(cl[-1], op[-1]) - lo[-1]   # lower shadow
        opp_shadow = hi[-1] - max(cl[-1], op[-1])
    else:
        shadow = hi[-1] - max(cl[-1], op[-1])   # upper shadow
        opp_shadow = min(cl[-1], op[-1]) - lo[-1]

    if shadow < 2.0 * max(body, avg_body * 0.2):
        return CandlePatternResult("none", "none", 0.0, "")
    if body > shadow * 0.6:
        return CandlePatternResult("none", "none", 0.0, "")

    quality = min(1.0, shadow / (body + 1e-10) / 5.0)  # longer shadow = higher quality
    # Penalise large opposite shadow
    if opp_shadow > shadow * 0.3:
        quality *= 0.8

    direction = "bull" if side == "bull" else "bear"
    return CandlePatternResult(
        "pin_bar", direction, quality,
        f"Пин-бар (тень×{shadow/max(body,1e-8):.1f} > тела)"
    )


def _engulfing(
    op: np.ndarray, hi: np.ndarray, lo: np.ndarray, cl: np.ndarray,
    avg_body: float, side: str,
) -> CandlePatternResult:
    """
    Engulfing: small candle against trend, then large candle in trend direction
    that engulfs the previous candle's body.
    """
    if len(op) < 2:
        return CandlePatternResult("none", "none", 0.0, "")

    prev_body  = cl[-2] - op[-2]   # signed
    curr_body  = cl[-1] - op[-1]   # signed

    # For bull side: prev should be bearish, curr should be bullish
    if side == "bull":
        if prev_body >= 0 or curr_body <= 0:
            return CandlePatternResult("none", "none", 0.0, "")
        # Curr engulfs prev body
        engulfs = cl[-1] > op[-2] and op[-1] < cl[-2]
    else:
        if prev_body <= 0 or curr_body >= 0:
            return CandlePatternResult("none", "none", 0.0, "")
        engulfs = cl[-1] < op[-2] and op[-1] > cl[-2]

    if not engulfs:
        return CandlePatternResult("none", "none", 0.0, "")

    ratio = abs(curr_body) / (abs(prev_body) + 1e-10)
    quality = min(1.0, (ratio - 1.0) * 0.5 + 0.6)  # scales from 0.6 at ratio=1.0

    direction = "bull" if side == "bull" else "bear"
    return CandlePatternResult(
        "engulfing", direction, quality,
        f"Поглощение (тело ×{ratio:.1f} от предыдущей)"
    )


def _hammer(
    op: np.ndarray, hi: np.ndarray, lo: np.ndarray, cl: np.ndarray,
    avg_body: float, side: str,
) -> CandlePatternResult:
    """
    Hammer / shooting star: small body, long shadow toward level, minimal opposite shadow.
    """
    body  = abs(cl[-1] - op[-1])
    total = hi[-1] - lo[-1]
    if total == 0:
        return CandlePatternResult("none", "none", 0.0, "")

    body_r = body / total

    if side == "bull":
        shadow = min(cl[-1], op[-1]) - lo[-1]   # lower shadow
        opp    = hi[-1] - max(cl[-1], op[-1])   # upper shadow
    else:
        shadow = hi[-1] - max(cl[-1], op[-1])   # upper shadow
        opp    = min(cl[-1], op[-1]) - lo[-1]   # lower shadow

    # Hammer: shadow at least 2× body, body < 40% of total range, small opposite
    if shadow < 2.0 * max(body, avg_body * 0.15):
        return CandlePatternResult("none", "none", 0.0, "")
    if body_r > 0.40:
        return CandlePatternResult("none", "none", 0.0, "")
    if opp > shadow * 0.5:
        return CandlePatternResult("none", "none", 0.0, "")

    quality = min(1.0, shadow / total * 2.0)
    direction = "bull" if side == "bull" else "bear"
    return CandlePatternResult(
        "hammer", direction, quality,
        f"Молот (тень {shadow/total*100:.0f}% диапазона)"
    )
