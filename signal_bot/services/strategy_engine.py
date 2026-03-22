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

    # ── Detailed condition log (one line per strategy) ────────────────────────
    _log_conditions(eng)

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


def _log_conditions(eng: "EngineResult") -> None:
    """Log per-strategy condition breakdown as searchable one-liners."""
    strategies: dict = eng.debug.get("strategies", {})
    if not strategies:
        return

    ctx_note = eng.debug.get("ctx_macro_note", "")
    ctx_up_1m   = eng.debug.get("ctx_up_1m", False)
    ctx_dn_1m   = eng.debug.get("ctx_dn_1m", False)
    ctx_macro_up = eng.debug.get("ctx_macro_up", False)
    ctx_macro_dn = eng.debug.get("ctx_macro_dn", False)
    conf_before  = eng.debug.get("conf_before_multipliers", 0)
    conf_after   = eng.debug.get("conf_after_multipliers", eng.confidence_raw)
    threshold    = eng.debug.get("min_threshold", 46)
    used_tier    = eng.debug.get("used_tier", "?")
    ind          = eng.debug.get("indicators", {})
    lvl          = eng.debug.get("levels", {})

    logger.info(
        "  ┌─ MODE=%s(%s%%) tier=%s | ctx_1m=%s%s macro=%s%s [%s]",
        eng.market_mode,
        round(eng.market_mode_strength),
        used_tier,
        "↑" if ctx_up_1m  else "·",
        "↓" if ctx_dn_1m  else "·",
        "↑" if ctx_macro_up else "·",
        "↓" if ctx_macro_dn else "·",
        ctx_note,
    )
    logger.info(
        "  │  IND: EMA5=%.6f EMA13=%.6f RSI=%.1f Stoch=%.0f/%.0f ATR_ratio=%.3f BB_bw=%.5f",
        ind.get("ema5", 0), ind.get("ema13", 0), ind.get("rsi", 0),
        ind.get("stoch_k", 0), ind.get("stoch_d", 0),
        ind.get("atr_ratio", 0), ind.get("bb_bw", 0),
    )
    logger.info(
        "  │  LVL: sup=%.6f(%.3f%%) res=%.6f(%.3f%%) n_sup=%d n_res=%d",
        lvl.get("nearest_sup", 0), lvl.get("dist_sup_pct", 0),
        lvl.get("nearest_res", 0), lvl.get("dist_res_pct", 0),
        lvl.get("n_supports", 0),  lvl.get("n_resistances", 0),
    )

    for sname, sd in strategies.items():
        if sd.get("skipped"):
            continue
        direction  = sd.get("direction", "NONE")
        conf       = sd.get("confidence", 0)
        met        = sd.get("conditions_met", 0)
        total      = sd.get("total", 0)
        pct        = sd.get("pct", 0)
        tier       = sd.get("tier", "?")
        early_rej  = sd.get("early_reject")
        conds: dict = sd.get("conditions", {})

        if early_rej:
            logger.info("  │  [%s/%s] SKIPPED early_reject=%s", sname, tier, early_rej)
            continue

        cond_parts = []
        for k, v in conds.items():
            if isinstance(v, bool):
                cond_parts.append(f"{'✓' if v else '✗'}{k}")
            elif k == "pattern_type":
                cond_parts.append(f"pat={v}")

        marker = "►" if sname == eng.strategy_name else "│"
        logger.info(
            "  %s [%s/%s] %s conf=%.0f met=%d/%d(%d%%) | %s",
            marker, sname, tier, direction, conf, met, total, pct,
            "  ".join(cond_parts) or "—",
        )

    if eng.direction != "NO_SIGNAL":
        logger.info(
            "  └─ RESULT %s conf %.0f→%.0f (thr=%d) expiry=%s",
            eng.direction, conf_before, conf_after, threshold, eng.expiry_hint,
        )
    else:
        logger.info("  └─ NO_SIGNAL: %s", eng.reasoning)


def _mode_to_regime(mode: str) -> str:
    """Map new market mode names to old regime strings used by signal formatter."""
    return {
        "TRENDING_UP":   "uptrend",
        "TRENDING_DOWN": "downtrend",
        "RANGE":         "range",
        "VOLATILE":      "chaotic_noise",
        "SQUEEZE":       "range",
    }.get(mode, "range")
