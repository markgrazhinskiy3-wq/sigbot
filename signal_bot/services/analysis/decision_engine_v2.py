"""
Decision Engine V2 — Pattern-First Architecture

Flow:
  1. Prepare data (15s df, 1m ctx, optional 5m)
  2. Detect levels v2 (with metadata)
  3. Run 4 pattern detectors in parallel
  4. Apply global filters to each candidate
  5. Filter by user-selected expiry
  6. Score patterns (pattern_quality × 0.60 + level_quality × 0.25 + context × 0.15)
  7. Pick best pattern above threshold
  8. Return EngineResult (same interface as v1 — no changes to strategy_engine.py needed)

Patterns:
  - level_rejection       (fit: 1m)
  - false_breakout        (fit: 2m, also 1m if fast)
  - compression_breakout  (fit: 1m, 2m)
  - impulse_pullback      (fit: 2m, also 1m if recent)

Thresholds:
  - 1m expiry: min score 68
  - 2m expiry: min score 65
  - raised (after 2 losses): +5 to both

EMA / RSI used ONLY as context weighting — never as pattern trigger.
"""
from __future__ import annotations
import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass, field

from .decision_engine              import EngineResult   # reuse same return type
from .levels_v2                    import detect_levels_v2, Level
from .pattern_level_rejection      import detect_level_rejection
from .pattern_compression_breakout import detect_compression_breakout
from .global_filters_v2            import apply_global_filters
from .impulse_pullback             import impulse_pullback_strategy
from .false_breakout               import false_breakout_strategy

logger = logging.getLogger(__name__)

# ── Score thresholds ──────────────────────────────────────────────────────────
_THRESHOLD_1M   = 68.0
_THRESHOLD_2M   = 65.0
_RAISED_BONUS   = 5.0     # added to threshold after 2 losses

# ── Expiry → fit_for mapping ─────────────────────────────────────────────────
_EXPIRY_FIT: dict[str, str] = {
    "1m": "1m",
    "2m": "2m",
}


def run_decision_engine_v2(
    df1m: pd.DataFrame,           # 15s candles (naming kept for compatibility)
    df5m: pd.DataFrame | None = None,
    df1m_ctx: pd.DataFrame | None = None,   # 1m resampled
    raised_threshold: bool = False,
    n_bars_15s: int = 0,
    n_bars_1m:  int = 0,
    n_bars_5m:  int = 0,
    expiry: str = "1m",           # "1m" | "2m" — user-selected expiry
) -> EngineResult:
    """
    Pattern-first signal engine.  Drop-in replacement for run_decision_engine().
    Caller (strategy_engine.py) must pass expiry from the user's selection.
    """
    df     = df1m    # 15s candles
    n      = len(df)
    price  = float(df["close"].iloc[-1])
    expiry = expiry if expiry in ("1m", "2m") else "1m"

    min_score = (
        _THRESHOLD_1M if expiry == "1m" else _THRESHOLD_2M
    ) + (_RAISED_BONUS if raised_threshold else 0.0)

    # ── Sanity ────────────────────────────────────────────────────────────────
    if n < 15:
        return _no_signal("too_few_candles", price, debug={"n": n, "expiry": expiry})

    # ── 1. Level detection v2 ─────────────────────────────────────────────────
    supports, resistances = detect_levels_v2(df, df1m_ctx)
    logger.debug(
        "Levels v2: %d supports, %d resistances | price=%.6f",
        len(supports), len(resistances), price,
    )

    levels_debug = _levels_debug(supports, resistances, price)

    # ── 2. Compute lightweight context (for scoring only) ─────────────────────
    ctx = _compute_context(df, df5m, df1m_ctx)

    # ── 3. Run all 4 pattern detectors ────────────────────────────────────────
    candidates: list[dict] = []

    # ─ 3a. Level Rejection ────────────────────────────────────────────────────
    lr = detect_level_rejection(df, supports, resistances)
    if lr.direction != "NONE":
        candidates.append(_wrap(lr, "level_rejection", supports, resistances))
    logger.debug("Pattern level_rejection: %s score=%.1f | %s",
                 lr.direction, lr.score, lr.rejection_reason or lr.debug.get(lr.direction.lower(), {}).get("reason", ""))

    # ─ 3b. False Breakout ─────────────────────────────────────────────────────
    sup_prices = [lvl.price for lvl in supports]
    res_prices = [lvl.price for lvl in resistances]
    fb = false_breakout_strategy(df, sup_prices, res_prices)
    if fb.direction in ("buy", "sell"):
        fb_result = _wrap_fb(fb, supports, resistances)
        candidates.append(fb_result)
    logger.debug("Pattern false_breakout: %s buy=%.1f sell=%.1f",
                 fb.direction, fb.buy_score, fb.sell_score)

    # ─ 3c. Compression Breakout ───────────────────────────────────────────────
    cb = detect_compression_breakout(df)
    if cb.direction != "NONE":
        candidates.append(_wrap(cb, "compression_breakout", supports, resistances))
    logger.debug("Pattern compression_breakout: %s score=%.1f | %s",
                 cb.direction, cb.score, cb.rejection_reason)

    # ─ 3d. Impulse Pullback ───────────────────────────────────────────────────
    ip = impulse_pullback_strategy(df)
    if ip.direction in ("buy", "sell"):
        ip_result = _wrap_ip(ip, supports, resistances)
        candidates.append(ip_result)
    logger.debug("Pattern impulse_pullback: %s buy=%.1f sell=%.1f",
                 ip.direction, ip.buy_score, ip.sell_score)

    # ── 4. Apply global filters ───────────────────────────────────────────────
    valid: list[dict] = []
    filter_log: list[str] = []

    for cand in candidates:
        filt = apply_global_filters(df, cand["direction"], supports, resistances,
                                    pattern_name=cand["name"])
        if not filt.passed:
            reason = f"{cand['name']} {cand['direction']} REJECTED by filter: {'; '.join(filt.reasons)}"
            filter_log.append(reason)
            logger.debug(reason)
            continue

        # Apply soft penalty
        cand["score"] = max(0.0, cand["score"] - filt.score_penalty)
        cand["filter_penalty"] = filt.score_penalty
        cand["filter_reasons"] = filt.reasons
        valid.append(cand)

    # ── 5. Filter by expiry ───────────────────────────────────────────────────
    expiry_filtered: list[dict] = []
    for cand in valid:
        if expiry in cand.get("fit_for", [expiry]):
            expiry_filtered.append(cand)
        else:
            logger.debug(
                "Pattern %s %s filtered out — fit_for=%s, user_expiry=%s",
                cand["name"], cand["direction"], cand.get("fit_for"), expiry,
            )

    # ── 6. Apply context scoring ──────────────────────────────────────────────
    for cand in expiry_filtered:
        ctx_score = _context_score(cand["direction"], ctx)
        # Context = 15% of final score
        cand["final_score"] = round(
            cand["score"] * 0.85 + ctx_score * 0.15,
            1,
        )
        cand["ctx_score"] = ctx_score

    # ── 7. Pick best pattern ──────────────────────────────────────────────────
    if not expiry_filtered:
        return _no_signal(
            f"no_patterns_passed (candidates={len(candidates)}, after_filter={len(valid)})",
            price,
            debug={
                "expiry": expiry,
                "candidates_checked":  [_cand_summary(c) for c in candidates],
                "filter_rejections":   filter_log,
                "levels":              levels_debug,
                "context":             ctx,
                "n_15s":               n,
                "n_1m":                n_bars_1m,
                "n_5m":                n_bars_5m,
            },
        )

    best = max(expiry_filtered, key=lambda c: c["final_score"])
    score = best["final_score"]

    # ── 8. Threshold check ────────────────────────────────────────────────────
    if score < min_score:
        return _no_signal(
            f"score_below_threshold: {score:.1f} < {min_score:.1f} (best={best['name']})",
            price,
            debug={
                "expiry":         expiry,
                "best_pattern":   best["name"],
                "best_direction": best["direction"],
                "best_score":     score,
                "min_score":      min_score,
                "all_patterns":   [_cand_summary(c) for c in expiry_filtered],
                "levels":         levels_debug,
                "context":        ctx,
                "n_15s":          n,
            },
        )

    # ── 9. Build EngineResult ─────────────────────────────────────────────────
    direction  = best["direction"]
    stars      = _to_stars(score)
    quality    = "strong" if score >= 75 else "moderate" if score >= 65 else "weak"
    expiry_out = expiry

    # Build human reason
    reason_parts = [
        f"Pattern: {best['name']}",
        f"Score: {score:.0f}/100",
    ]
    if best.get("level_detail"):
        reason_parts.append(best["level_detail"])

    debug_out = {
        "engine":        "v2_pattern_first",
        "expiry":        expiry,
        "direction":     direction,
        "best_pattern":  best["name"],
        "final_score":   score,
        "raw_score":     best["score"],
        "ctx_score":     best.get("ctx_score", 0.0),
        "min_score":     min_score,
        "raised":        raised_threshold,
        "pattern_q":     best.get("pattern_quality", 0.0),
        "level_q":       best.get("level_quality", 0.0),
        "filter_penalty": best.get("filter_penalty", 0.0),
        "all_patterns":  [_cand_summary(c) for c in expiry_filtered],
        "filter_log":    filter_log,
        "levels":        levels_debug,
        "context":       ctx,
        "pattern_debug": best.get("pattern_debug", {}),
        "n_15s":         n,
        "n_1m":          n_bars_1m,
        "n_5m":          n_bars_5m,
    }

    logger.info(
        "V2 Signal: %s | pattern=%s | score=%.1f (min=%.0f) | stars=%d | expiry=%s",
        direction, best["name"], score, min_score, stars, expiry_out,
    )
    _log_v2_debug(best, expiry_filtered, filter_log, levels_debug, ctx, score, min_score)

    return EngineResult(
        direction=direction,
        confidence_raw=score,
        stars=stars,
        quality=quality,
        strategy_name=best["name"],
        market_mode=ctx.get("regime", "RANGE"),
        market_mode_strength=ctx.get("regime_strength", 50.0),
        reasoning="; ".join(reason_parts),
        conditions_met=int(score),
        total_conditions=100,
        expiry_hint=expiry_out,
        debug=debug_out,
    )


# ── Pattern wrappers ──────────────────────────────────────────────────────────

def _wrap(result, name: str, supports: list, resistances: list) -> dict:
    """Wrap a native PatternResult into a scoring dict."""
    direction = result.direction  # "BUY" | "SELL"
    opp_room  = _opposite_room(direction, result.score, supports, resistances)
    level_detail = ""

    if hasattr(result, "debug"):
        dir_key = direction.lower()
        d = result.debug.get(dir_key, {})
        lp = d.get("level_price")
        if lp:
            level_detail = f"Level {lp:.5f} ({d.get('touches', '?')}x)"

    return {
        "name":            name,
        "direction":       direction,
        "score":           result.score,
        "fit_for":         result.fit_for,
        "pattern_quality": result.pattern_quality,
        "level_quality":   result.level_quality,
        "room_quality":    opp_room,
        "level_detail":    level_detail,
        "pattern_debug":   result.debug,
    }


def _wrap_fb(fb, supports: list, resistances: list) -> dict:
    """Wrap FalseBreakoutResult."""
    direction = "BUY" if fb.direction == "buy" else "SELL"
    score     = fb.buy_score if direction == "BUY" else fb.sell_score

    # Remap 0-100 false_breakout score to 0-100 with pattern/level split
    pattern_q = score * 0.75
    level_q   = score * 0.25

    return {
        "name":            "false_breakout",
        "direction":       direction,
        "score":           score,
        "fit_for":         ["2m", "1m"],   # better for 2m, usable for 1m
        "pattern_quality": round(pattern_q, 1),
        "level_quality":   round(level_q, 1),
        "room_quality":    50.0,
        "level_detail":    fb.explanation,
        "pattern_debug":   fb.debug,
    }


def _wrap_ip(ip, supports: list, resistances: list) -> dict:
    """Wrap ImpulsePullbackResult."""
    direction = "BUY" if ip.direction == "buy" else "SELL"
    score     = ip.buy_score if direction == "BUY" else ip.sell_score

    pattern_q = score * 0.80
    level_q   = 50.0   # impulse doesn't need a level

    return {
        "name":            "impulse_pullback",
        "direction":       direction,
        "score":           score,
        "fit_for":         ["2m", "1m"],  # better for 2m
        "pattern_quality": round(pattern_q, 1),
        "level_quality":   round(level_q, 1),
        "room_quality":    50.0,
        "level_detail":    ip.explanation,
        "pattern_debug":   ip.debug,
    }


# ── Context scoring ───────────────────────────────────────────────────────────

def _compute_context(
    df: pd.DataFrame,
    df5m: pd.DataFrame | None,
    df1m: pd.DataFrame | None,
) -> dict:
    """Compute lightweight context for signal weighting. NOT a signal source."""
    n  = len(df)
    cl = df["close"].values
    op = df["open"].values
    hi = df["high"].values
    lo = df["low"].values

    # EMA5 / EMA13 (filter only)
    ema5  = _ema(cl, 5)
    ema13 = _ema(cl, 13)
    ema_spread = abs(ema5 - ema13) / (cl[-1] + 1e-10) * 100

    # RSI-14 (filter only)
    rsi = _rsi(cl, 14)

    # ATR ratio
    atr_recent   = float(np.mean((hi - lo)[-10:]))
    atr_baseline = float(np.mean((hi - lo)[-30:])) or 1e-8
    atr_ratio    = atr_recent / atr_baseline

    # Market regime estimation
    regime, strength = _estimate_regime(cl, ema5, ema13, atr_ratio)

    # 1m macro context
    ctx_up_1m = ctx_dn_1m = False
    if df1m is not None and len(df1m) >= 5:
        c1m = df1m["close"].values
        ctx_up_1m = c1m[-1] > c1m[-5]
        ctx_dn_1m = c1m[-1] < c1m[-5]

    # 5m macro
    ctx_macro_up = ctx_macro_dn = False
    if df5m is not None and len(df5m) >= 3:
        c5m = df5m["close"].values
        ctx_macro_up = c5m[-1] > c5m[-3]
        ctx_macro_dn = c5m[-1] < c5m[-3]

    return {
        "ema5":          round(float(ema5), 6),
        "ema13":         round(float(ema13), 6),
        "ema_spread":    round(ema_spread, 4),
        "rsi":           round(float(rsi), 1),
        "atr_ratio":     round(atr_ratio, 3),
        "regime":        regime,
        "regime_strength": round(strength, 1),
        "ctx_up_1m":     ctx_up_1m,
        "ctx_dn_1m":     ctx_dn_1m,
        "ctx_macro_up":  ctx_macro_up,
        "ctx_macro_dn":  ctx_macro_dn,
    }


def _context_score(direction: str, ctx: dict) -> float:
    """
    Score 0-100: how well the context supports the pattern direction.
    This is FILTER/WEIGHT only — never the reason for a signal.
    """
    score   = 50.0   # neutral baseline
    is_buy  = direction == "BUY"
    rsi     = ctx.get("rsi", 50.0)
    ctx_1m  = ctx.get("ctx_up_1m" if is_buy else "ctx_dn_1m", False)
    ctx_mac = ctx.get("ctx_macro_up" if is_buy else "ctx_macro_dn", False)

    # RSI alignment (gentle nudge only)
    if is_buy and rsi < 50:
        score += min(15.0, (50 - rsi) * 0.5)
    elif not is_buy and rsi > 50:
        score += min(15.0, (rsi - 50) * 0.5)

    # 1m context alignment
    if ctx_1m:
        score += 15.0

    # 5m macro alignment
    if ctx_mac:
        score += 10.0

    # ATR: avoid dead or hyper-volatile
    atr = ctx.get("atr_ratio", 1.0)
    if atr < 0.4:
        score -= 15.0
    elif atr > 2.5:
        score -= 10.0

    return float(max(0.0, min(100.0, score)))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _opposite_room(direction: str, score: float, supports: list, resistances: list) -> float:
    """Placeholder — room check done in global filters."""
    return score * 0.5


def _levels_debug(supports: list, resistances: list, price: float) -> dict:
    nearest_sup = supports[0].price  if supports    else price * 0.995
    nearest_res = resistances[0].price if resistances else price * 1.005
    return {
        "n_supports":    len(supports),
        "n_resistances": len(resistances),
        "nearest_sup":   round(nearest_sup, 6),
        "nearest_res":   round(nearest_res, 6),
        "dist_sup_pct":  round(abs(price - nearest_sup) / price * 100, 4),
        "dist_res_pct":  round(abs(nearest_res - price) / price * 100, 4),
        "supports":  [{"price": l.price, "touches": l.touches, "fresh": l.is_fresh, "broken": l.is_broken}
                      for l in supports[:5]],
        "resistances": [{"price": l.price, "touches": l.touches, "fresh": l.is_fresh, "broken": l.is_broken}
                        for l in resistances[:5]],
    }


def _cand_summary(c: dict) -> dict:
    return {
        "name":      c["name"],
        "direction": c["direction"],
        "score":     c.get("final_score", c.get("score", 0.0)),
        "fit_for":   c.get("fit_for", []),
    }


def _to_stars(score: float) -> int:
    if score >= 88: return 5
    if score >= 80: return 4
    if score >= 70: return 3
    if score >= 65: return 2
    return 1


def _estimate_regime(cl, ema5: float, ema13: float, atr_ratio: float) -> tuple[str, float]:
    spread_pct = abs(ema5 - ema13) / (cl[-1] + 1e-10) * 100
    if spread_pct < 0.002 or atr_ratio < 0.4:
        return "RANGE", 30.0
    if ema5 > ema13 and spread_pct > 0.005:
        return "TRENDING_UP", min(90.0, spread_pct / 0.01 * 50.0)
    if ema5 < ema13 and spread_pct > 0.005:
        return "TRENDING_DOWN", min(90.0, spread_pct / 0.01 * 50.0)
    return "RANGE", 50.0


def _ema(series: np.ndarray, period: int) -> float:
    if len(series) < period:
        return float(series[-1])
    k = 2.0 / (period + 1)
    val = float(series[0])
    for v in series[1:]:
        val = float(v) * k + val * (1 - k)
    return val


def _rsi(series: np.ndarray, period: int = 14) -> float:
    if len(series) < period + 1:
        return 50.0
    deltas = np.diff(series.astype(float))
    gains  = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_g  = np.mean(gains[-period:])
    avg_l  = np.mean(losses[-period:])
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - 100.0 / (1.0 + rs)


def _no_signal(reason: str, price: float = 0.0, debug: dict | None = None) -> EngineResult:
    logger.info("V2 NO_SIGNAL: %s", reason)
    return EngineResult(
        direction="NO_SIGNAL",
        confidence_raw=0.0,
        stars=0,
        quality="none",
        strategy_name="",
        market_mode="RANGE",
        market_mode_strength=0.0,
        reasoning=reason,
        conditions_met=0,
        total_conditions=100,
        expiry_hint="1m",
        debug={
            **(debug or {}),
            "engine":        "v2_pattern_first",
            "reject_reason": reason,
        },
    )


# ── Debug logger ──────────────────────────────────────────────────────────────

def _log_v2_debug(
    best: dict,
    all_valid: list,
    filter_log: list,
    levels: dict,
    ctx: dict,
    score: float,
    min_score: float,
) -> None:
    logger.info(
        "  ┌─ V2 ENGINE | pattern=%s direction=%s score=%.1f (min=%.0f)",
        best["name"], best["direction"], score, min_score,
    )
    logger.info(
        "  │  CTX: regime=%s EMA_spread=%.4f%% RSI=%.1f ATR_ratio=%.2f "
        "ctx_1m=%s%s ctx_mac=%s%s",
        ctx.get("regime"), ctx.get("ema_spread"), ctx.get("rsi"), ctx.get("atr_ratio"),
        "↑" if ctx.get("ctx_up_1m")  else "·",
        "↓" if ctx.get("ctx_dn_1m")  else "·",
        "↑" if ctx.get("ctx_macro_up") else "·",
        "↓" if ctx.get("ctx_macro_dn") else "·",
    )
    logger.info(
        "  │  LEVELS: sup=%d(nearest=%.5f %.4f%%) res=%d(nearest=%.5f %.4f%%)",
        levels.get("n_supports", 0), levels.get("nearest_sup", 0), levels.get("dist_sup_pct", 0),
        levels.get("n_resistances", 0), levels.get("nearest_res", 0), levels.get("dist_res_pct", 0),
    )
    for cand in all_valid:
        marker = "►" if cand is best else "│"
        logger.info(
            "  %s  PATTERN %-22s %s score=%.1f (pat_q=%.0f lv_q=%.0f ctx=%.0f)",
            marker, cand["name"], cand["direction"],
            cand.get("final_score", cand.get("score", 0)),
            cand.get("pattern_quality", 0), cand.get("level_quality", 0),
            cand.get("ctx_score", 0),
        )
    for rej in filter_log:
        logger.info("  │  FILTER REJECT: %s", rej)
    logger.info(
        "  └─ RESULT %s score=%.1f stars=%d",
        best["direction"], score, _to_stars(score),
    )
