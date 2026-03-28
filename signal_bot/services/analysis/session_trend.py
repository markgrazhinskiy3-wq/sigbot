"""
Session Trend Detector
======================
Determines the overall market direction (USD strength / weakness) by checking
the 5-minute EMA5 vs EMA13 trend on a basket of three major USD pairs.

Result
------
'BULL'    — 2+ reference pairs trending up   → suppress SELL signals
'BEAR'    — 2+ reference pairs trending down → suppress BUY  signals
'NEUTRAL' — mixed or insufficient data       → allow both directions

Why USD pairs?
  EUR/USD, GBP/USD, AUD/USD all move together when USD strengthens/weakens.
  If 2 of 3 are trending up → broad USD weakness → most pairs rise → SELL signals lose.
  If 2 of 3 are trending down → broad USD strength → most pairs fall → BUY signals lose.
"""
from __future__ import annotations

import logging
from typing import Literal

logger = logging.getLogger(__name__)

# OTC symbols used as the market-direction basket
_REF_PAIRS = ["#EURUSD_otc", "#GBPUSD_otc", "#AUDUSD_otc"]

# Minimum number of 5m candles required for a reliable EMA13
_MIN_5M_CANDLES = 13

# Minimum 15s candles per pair before we bother resampling (~15 minutes of data)
_MIN_15S_CANDLES = 60

# Small buffer so flat EMA pairs don't flip direction on noise
_EMA_BUFFER = 0.00005   # 0.5 pip equivalent in ratio terms

SessionDirection = Literal["BULL", "BEAR", "NEUTRAL"]


# ── EMA helper ────────────────────────────────────────────────────────────────

def _ema(values: list[float], period: int) -> float:
    """Classic exponential moving average (Wilder-style seed from first value)."""
    if not values:
        return 0.0
    if len(values) < period:
        return values[-1]
    k   = 2.0 / (period + 1)
    val = values[0]
    for v in values[1:]:
        val = v * k + val * (1 - k)
    return val


# ── Main API ──────────────────────────────────────────────────────────────────

def get_session_direction(candles_map: dict[str, list[dict]]) -> SessionDirection:
    """
    Determine the current session direction from the reference USD-pair basket.

    Parameters
    ----------
    candles_map : dict[symbol, list[candle_dict]]
        Raw 15-second candles already fetched by the scanner/paper-runner.

    Returns
    -------
    'BULL'    if ≥2 reference pairs show EMA5 > EMA13 on the 5m chart
    'BEAR'    if ≥2 reference pairs show EMA5 < EMA13 on the 5m chart
    'NEUTRAL' otherwise (mixed signals or not enough data)
    """
    from services.candle_cache import resample_to_1m, resample_to_5m   # lazy import to avoid circulars

    up_count   = 0
    down_count = 0
    checked    = 0

    for sym in _REF_PAIRS:
        candles = candles_map.get(sym)
        if not candles or len(candles) < _MIN_15S_CANDLES:
            logger.debug("SessionTrend: %s — not enough candles (%d)", sym, len(candles) if candles else 0)
            continue

        try:
            candles_1m = resample_to_1m(candles)
            candles_5m = resample_to_5m(candles_1m)
            if len(candles_5m) < _MIN_5M_CANDLES:
                logger.debug("SessionTrend: %s — only %d 5m candles", sym, len(candles_5m))
                continue

            closes = [float(c["close"]) for c in candles_5m[-_MIN_5M_CANDLES:]]
            ema5  = _ema(closes, 5)
            ema13 = _ema(closes, 13)

            if ema13 == 0:
                continue

            ratio = ema5 / ema13
            checked += 1

            if ratio > 1 + _EMA_BUFFER:
                up_count += 1
                logger.debug("SessionTrend: %s UP  (EMA5=%.6f EMA13=%.6f ratio=%.6f)", sym, ema5, ema13, ratio)
            elif ratio < 1 - _EMA_BUFFER:
                down_count += 1
                logger.debug("SessionTrend: %s DOWN (EMA5=%.6f EMA13=%.6f ratio=%.6f)", sym, ema5, ema13, ratio)
            else:
                logger.debug("SessionTrend: %s FLAT (ratio=%.6f)", sym, ratio)

        except Exception as exc:
            logger.debug("SessionTrend: error for %s: %s", sym, exc)

    if checked == 0:
        logger.info("SessionTrend: no reference pairs available → NEUTRAL")
        return "NEUTRAL"

    if up_count >= 2:
        direction: SessionDirection = "BULL"
    elif down_count >= 2:
        direction = "BEAR"
    else:
        direction = "NEUTRAL"

    logger.info(
        "SessionTrend: checked=%d up=%d down=%d → %s",
        checked, up_count, down_count, direction,
    )
    return direction
