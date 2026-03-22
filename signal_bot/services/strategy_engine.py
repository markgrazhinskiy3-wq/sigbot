"""
Strategy Engine — entry point for signal calculation.
Validates candles, resamples to 1-min and 5-min, then runs the full decision engine.
"""
import logging
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services.analysis.candle_validator import validate_and_fix
from services.analysis.decision_engine  import run_decision_engine, EngineResult
from services.candle_cache              import resample_to_1m, resample_to_5m
from services.strategy_adaptation      import update_strategy_statuses
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SignalResult:
    direction:  str   # "BUY" | "SELL" | "NO_SIGNAL"
    confidence: int   # 0-5 stars for UI display
    details:    dict  # full analysis breakdown


async def calculate_signal(
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

    # Strategy adaptation: refresh statuses (cached 60s — no-op most of the time)
    try:
        await update_strategy_statuses()
    except Exception:
        pass  # never block signal calculation

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

    # ── Resample 15s → 1-min (intermediate context) ───────────────────────────
    df1m_ctx = None
    try:
        import pandas as pd
        candles_1m = resample_to_1m(candles)
        if len(candles_1m) >= 5:
            df1m_ctx = pd.DataFrame(candles_1m)
            for col in ("open", "high", "low", "close"):
                df1m_ctx[col] = df1m_ctx[col].astype(float)
            logger.debug("1-min candles: %d", len(df1m_ctx))
    except Exception as e:
        logger.warning("1-min resampling failed: %s", e)

    # ── Resample 15s → 5-min (macro context) ─────────────────────────────────
    df5m = None
    try:
        import pandas as pd
        candles_5m = resample_to_5m(candles)
        if len(candles_5m) >= 4:
            df5m = pd.DataFrame(candles_5m)
            for col in ("open", "high", "low", "close"):
                df5m[col] = df5m[col].astype(float)
            logger.debug("5-min candles: %d", len(df5m))
    except Exception as e:
        logger.warning("5-min resampling failed: %s", e)

    # ── Decision Engine ───────────────────────────────────────────────────────
    eng: EngineResult = run_decision_engine(
        df1m=df,
        df5m=df5m,
        df1m_ctx=df1m_ctx,
        raised_threshold=raised_threshold,
        n_bars_15s=len(df),
        n_bars_1m=len(df1m_ctx) if df1m_ctx is not None else 0,
        n_bars_5m=len(df5m)     if df5m     is not None else 0,
    )

    logger.info(
        "Signal result: %s | strategy=%s | conf_raw=%.0f | mode=%s | reason=%s",
        eng.direction,
        eng.strategy_name or "—",
        eng.confidence_raw,
        eng.market_mode,
        eng.reasoning if eng.direction == "NO_SIGNAL" else "ok",
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
            # last_close is used by outcome_tracker to determine WIN/LOSS
            "last_close":    val.last_close,
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
