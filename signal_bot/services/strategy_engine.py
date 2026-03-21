"""
Strategy Engine — entry point for signal calculation.
Validates candles, then delegates to the scoring engine.
"""
import logging
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services.analysis.candle_validator import validate_and_fix
from services.analysis.scoring_engine   import run_scoring_engine
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SignalResult:
    direction:  str   # "BUY" | "SELL" | "NO_SIGNAL"
    confidence: int   # 0-5 for UI display
    details:    dict  # full analysis breakdown


def calculate_signal(candles: list[dict]) -> SignalResult:
    """
    Receives a list of OHLC dicts:
        [{"open": float, "high": float, "low": float, "close": float}, ...]
    Validates, cleans, and runs the scoring engine.
    """
    n_raw = len(candles)

    # ── Validate & clean ──────────────────────────────────────────────────────
    df, val = validate_and_fix(candles)

    if not val.ok or df is None or len(df) < 10:
        logger.warning(
            "Not enough usable candles: raw=%d, clean=%d, issues=%s",
            n_raw, val.candles_after_clean, val.issues
        )
        return SignalResult("NO_SIGNAL", 0, {
            "error": "not_enough_candles",
            "candles_raw": n_raw,
            "candles_clean": val.candles_after_clean,
            "issues": val.issues,
            "debug": {
                "candles_count": n_raw,
                "candles_after_clean": val.candles_after_clean,
                "order": val.order,
                "issues": val.issues,
                "reject_reason": "validation_failed",
            }
        })

    logger.info(
        "Candles OK: %d raw → %d clean | order=%s | last=%.6f | avg_body=%.4f%%",
        n_raw, len(df), val.order, val.last_close, val.avg_body_pct
    )

    # ── Score ─────────────────────────────────────────────────────────────────
    result = run_scoring_engine(df)

    # Attach validation info to debug
    if "debug" in result:
        result["debug"]["candles_raw"]   = n_raw
        result["debug"]["candles_clean"] = len(df)
        result["debug"]["order"]         = val.order
        result["debug"]["avg_body_pct"]  = round(val.avg_body_pct, 5)

    return SignalResult(
        direction  = result["direction"],
        confidence = result["confidence_5"],
        details    = result,
    )
