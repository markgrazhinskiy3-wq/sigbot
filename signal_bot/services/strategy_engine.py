"""
Strategy Engine — entry point for signal calculation.
Validates candles, resamples to 5-min, then runs the full decision engine.
"""
import logging
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services.analysis.candle_validator import validate_and_fix
from services.analysis.decision_engine  import run_decision_engine, EngineResult
from services.candle_cache              import resample_to_5m
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SignalResult:
    direction:  str   # "BUY" | "SELL" | "NO_SIGNAL"
    confidence: int   # 0-5 stars for UI display
    details:    dict  # full analysis breakdown


def calculate_signal(
    candles: list[dict],
    raised_threshold: bool = False,
) -> SignalResult:
    """
    Receives a list of OHLC dicts:
        [{"open": float, "high": float, "low": float, "close": float, "time": int}, ...]
    Validates, cleans, resamples to 5-min, then runs the decision engine.

    Args:
        candles: list of 1-min OHLC candles from cache
        raised_threshold: True after 2 consecutive losses → raise min confidence to 70
    """
    n_raw = len(candles)

    # ── Validate & clean ──────────────────────────────────────────────────────
    df, val = validate_and_fix(candles)

    if not val.ok or df is None or len(df) < 15:
        logger.warning(
            "Not enough usable candles: raw=%d, clean=%d, issues=%s",
            n_raw, val.candles_after_clean, val.issues
        )
        return SignalResult("NO_SIGNAL", 0, {
            "direction":      "NO_SIGNAL",
            "error":          "not_enough_candles",
            "candles_raw":    n_raw,
            "candles_clean":  val.candles_after_clean,
            "reject_reason":  "validation_failed",
            "debug": {
                "candles_count":      n_raw,
                "candles_after_clean": val.candles_after_clean,
                "order":              val.order,
                "issues":             val.issues,
                "reject_reason":      "validation_failed",
            }
        })

    logger.info(
        "Candles OK: %d raw → %d clean | order=%s | last=%.6f | avg_body=%.4f%%",
        n_raw, len(df), val.order, val.last_close, val.avg_body_pct
    )

    # ── Resample to 5-min ──────────────────────────────────────────────────────
    df5m = None
    try:
        candles_5m = resample_to_5m(candles)
        if len(candles_5m) >= 4:
            import pandas as pd
            df5m = pd.DataFrame(candles_5m)
            df5m["open"]  = df5m["open"].astype(float)
            df5m["high"]  = df5m["high"].astype(float)
            df5m["low"]   = df5m["low"].astype(float)
            df5m["close"] = df5m["close"].astype(float)
            logger.debug("5-min candles: %d", len(df5m))
    except Exception as e:
        logger.warning("5-min resampling failed: %s", e)

    # ── Decision Engine ───────────────────────────────────────────────────────
    eng: EngineResult = run_decision_engine(
        df1m=df,
        df5m=df5m,
        raised_threshold=raised_threshold,
    )

    # ── Build details dict for signal_service ─────────────────────────────────
    details = {
        "direction":          eng.direction,
        "confidence_raw":     eng.confidence_raw,
        "confidence_5":       eng.stars,
        "signal_quality":     eng.quality,
        "primary_strategy":   eng.strategy_name,
        "market_mode":        eng.market_mode,
        "market_mode_strength": eng.market_mode_strength,
        "expiry_hint":        eng.expiry_hint,
        "reasoning":          eng.reasoning,
        "conditions_met":     eng.conditions_met,
        "total_conditions":   eng.total_conditions,
        # Legacy field aliases (for signal_service / format_signal_message compat)
        "regime":             _mode_to_regime(eng.market_mode),
        "reject_reason":      "" if eng.direction != "NO_SIGNAL" else eng.reasoning,
        "hard_conflicts":     [] if eng.direction != "NO_SIGNAL" else [eng.reasoning],
        "debug": {
            **eng.debug,
            "candles_raw":   n_raw,
            "candles_clean": len(df),
            "order":         val.order,
            "avg_body_pct":  round(val.avg_body_pct, 5),
        }
    }

    return SignalResult(
        direction  = eng.direction,
        confidence = eng.stars,
        details    = details,
    )


def _mode_to_regime(mode: str) -> str:
    """Map new market mode names to old regime strings used by signal formatter."""
    return {
        "TRENDING_UP":   "uptrend",
        "TRENDING_DOWN": "downtrend",
        "RANGE":         "range",
        "VOLATILE":      "chaotic_noise",
        "SQUEEZE":       "range",
    }.get(mode, "range")
