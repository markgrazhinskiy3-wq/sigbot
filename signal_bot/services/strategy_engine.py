"""
Strategy Engine — entry point for signal calculation.
Validates candles, resamples to 1-min and 5-min, then runs the full decision engine.
"""
import logging
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services.analysis.candle_validator  import validate_and_fix
from services.analysis.decision_engine  import run_decision_engine, EngineResult
from services.analysis.decision_engine_v2 import run_decision_engine_v2
from services.candle_cache               import resample_to_1m, resample_to_5m
from services.strategy_adaptation       import update_strategy_statuses
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
    expiry: str = "1m",
) -> SignalResult:
    """
    Receives a list of OHLC dicts:
        [{"open": float, "high": float, "low": float, "close": float, "time": int}, ...]
    Validates, cleans, resamples to 5-min, then runs the pattern-first decision engine v2.

    Args:
        candles:          list of 15s OHLC candles from cache
        raised_threshold: True after 2 consecutive losses → raise min score threshold
        expiry:           user-selected trade expiry — "1m" or "2m"
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
    candles_1m: list[dict] = []   # kept in scope so 5m block can reuse it
    try:
        import pandas as pd
        times_ok = sum(1 for c in candles if c.get("time", 0) > 0)
        logger.info(
            "1m resample input: %d candles, %d with valid time (first=%s last=%s)",
            len(candles), times_ok,
            candles[0].get("time") if candles else "N/A",
            candles[-1].get("time") if candles else "N/A",
        )
        candles_1m = resample_to_1m(candles)
        logger.info("1-min resample: %d raw 15s → %d 1m bars", len(candles), len(candles_1m))
        if len(candles_1m) >= 5:
            df1m_ctx = pd.DataFrame(candles_1m)
            for col in ("open", "high", "low", "close"):
                df1m_ctx[col] = df1m_ctx[col].astype(float)
        else:
            logger.warning(
                "1-min candles too few (%d) — ctx_1m disabled",
                len(candles_1m),
            )
    except Exception as e:
        logger.warning("1-min resampling failed: %s", e, exc_info=True)

    # ── Resample 1m → 5-min (macro context) ───────────────────────────────────
    # NOTE: resample_to_5m expects 1-min input, NOT raw 15s candles.
    # Using candles_1m (already resampled, with proper synthetic timestamps if needed)
    # ensures correct 5m grouping even when original candles have time=0.
    df5m = None
    try:
        import pandas as pd
        candles_5m = resample_to_5m(candles_1m)
        logger.info("5-min resample: %d 1m bars → %d 5m bars", len(candles_1m), len(candles_5m))
        if len(candles_5m) >= 3:
            df5m = pd.DataFrame(candles_5m)
            for col in ("open", "high", "low", "close"):
                df5m[col] = df5m[col].astype(float)
        else:
            logger.warning("5-min candles too few (%d) — df5m disabled", len(candles_5m))
    except Exception as e:
        logger.warning("5-min resampling failed: %s", e, exc_info=True)

    # ── Decision Engine V2 (Pattern-First) ────────────────────────────────────
    eng: EngineResult = run_decision_engine_v2(
        df1m=df,
        df5m=df5m,
        df1m_ctx=df1m_ctx,
        raised_threshold=raised_threshold,
        n_bars_15s=len(df),
        n_bars_1m=len(df1m_ctx) if df1m_ctx is not None else 0,
        n_bars_5m=len(df5m)     if df5m     is not None else 0,
        expiry=expiry,
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
    """Log per-pattern condition breakdown. Handles both v1 (strategies) and v2 (patterns) debug."""

    # ── V2 debug format (pattern-first) ──────────────────────────────────────
    if eng.debug.get("engine", "").startswith("v") and "pattern_first" in eng.debug.get("engine", ""):
        patterns  = eng.debug.get("all_patterns", [])
        ctx       = eng.debug.get("context", {})
        levels    = eng.debug.get("levels", {})
        filt_log  = eng.debug.get("filter_log", [])
        logger.info(
            "  ┌─ V2 MODE=%s(%.0f%%) expiry=%s score=%.1f(min=%.0f) raised=%s",
            eng.market_mode, eng.market_mode_strength,
            eng.debug.get("expiry", "?"), eng.confidence_raw,
            eng.debug.get("min_score", 0), eng.debug.get("raised", False),
        )
        logger.info(
            "  │  CTX: EMA_sp=%.4f%% RSI=%.1f ATR=%.2f 1m=%s%s mac=%s%s",
            ctx.get("ema_spread", 0), ctx.get("rsi", 50), ctx.get("atr_ratio", 1),
            "↑" if ctx.get("ctx_up_1m")  else "·",
            "↓" if ctx.get("ctx_dn_1m")  else "·",
            "↑" if ctx.get("ctx_macro_up") else "·",
            "↓" if ctx.get("ctx_macro_dn") else "·",
        )
        logger.info(
            "  │  LVL: sup=%d(%.5f/%.3f%%) res=%d(%.5f/%.3f%%)",
            levels.get("n_supports", 0), levels.get("nearest_sup", 0), levels.get("dist_sup_pct", 0),
            levels.get("n_resistances", 0), levels.get("nearest_res", 0), levels.get("dist_res_pct", 0),
        )
        for p in patterns:
            marker = "►" if p.get("name") == eng.strategy_name and eng.direction != "NO_SIGNAL" else "│"
            logger.info(
                "  %s  PATTERN %-22s %-4s score=%.1f fit=%s",
                marker, p.get("name", "?"), p.get("direction", "?"),
                p.get("score", 0), p.get("fit_for", []),
            )
        for r in filt_log:
            logger.info("  │  FILTER: %s", r)
        if eng.direction != "NO_SIGNAL":
            logger.info("  └─ RESULT %s score=%.1f expiry=%s", eng.direction, eng.confidence_raw, eng.expiry_hint)
        else:
            logger.info("  └─ NO_SIGNAL: %s", eng.reasoning)
        return

    # ── V1 debug format (legacy indicator-based) ──────────────────────────────
    strategies: dict = eng.debug.get("strategies", {})
    if not strategies:
        return

    ctx_note     = eng.debug.get("ctx_macro_note", "")
    ctx_up_1m    = eng.debug.get("ctx_up_1m", False)
    ctx_dn_1m    = eng.debug.get("ctx_dn_1m", False)
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
        eng.market_mode, round(eng.market_mode_strength), used_tier,
        "↑" if ctx_up_1m  else "·", "↓" if ctx_dn_1m  else "·",
        "↑" if ctx_macro_up else "·", "↓" if ctx_macro_dn else "·",
        ctx_note,
    )
    logger.info(
        "  │  IND: EMA5=%.6f EMA13=%.6f RSI=%.1f ATR_ratio=%.3f",
        ind.get("ema5", 0), ind.get("ema13", 0), ind.get("rsi", 0),
        ind.get("atr_ratio", 0),
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
        direction = sd.get("direction", "NONE")
        conf      = sd.get("confidence", 0)
        met       = sd.get("conditions_met", 0)
        total     = sd.get("total", 0)
        pct       = sd.get("pct", 0)
        tier      = sd.get("tier", "?")
        early_rej = sd.get("early_reject")
        conds     = sd.get("conditions", {})

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
