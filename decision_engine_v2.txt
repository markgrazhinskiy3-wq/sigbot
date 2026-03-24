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

# ── Per-strategy min score thresholds (v5) ───────────────────────────────────
# Replaces global threshold + stacked penalties — cleaner, more predictable.
# IP gets stricter threshold; LR and FB get lower thresholds to increase signal flow.
_STRATEGY_THRESHOLDS: dict[str, dict[str, float]] = {
    "impulse_pullback":     {"1m": 76.0, "2m": 73.0},
    "level_rejection":      {"1m": 68.0, "2m": 65.0},
    "false_breakout":       {"1m": 68.0, "2m": 65.0},
    "compression_breakout": {"1m": 68.0, "2m": 65.0},
}

# IP score cap: data shows score>88 has same WR as 85-89 (late trend entries).
_IP_SCORE_CAP = 88.0

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
    logger.debug(
        "Pattern impulse_pullback: %s buy=%.1f sell=%.1f gap=%.1f countertrend=%s",
        ip.direction, ip.buy_score, ip.sell_score,
        ip.debug.get("direction_gap", 0), ip.debug.get("countertrend", False),
    )

    # ── 3e. IP score cap (applied before ANY filter) ──────────────────────────
    # Ensures cap is visible in debug even if pattern is later filtered out.
    # Fixes: BHD/CNY IP score=100 was not being capped.
    for cand in candidates:
        if cand["name"] == "impulse_pullback" and cand["score"] > _IP_SCORE_CAP:
            cand["score"] = _IP_SCORE_CAP
            cand.setdefault("filter_reasons", []).append(
                f"ip_score_cap: capped at {_IP_SCORE_CAP:.0f} (raw score was >{_IP_SCORE_CAP:.0f})"
            )

    # ── 4. Apply global filters (now with expiry) ─────────────────────────────
    valid: list[dict] = []
    filter_log: list[str] = []

    for cand in candidates:
        filt = apply_global_filters(
            df, cand["direction"], supports, resistances,
            pattern_name=cand["name"],
            expiry=expiry,
        )
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

    # ── 4.5 Impulse-specific guards: countertrend & RANGE rejection ───────────
    # These checks need context (regime), so they run after basic filters but
    # before context scoring. We compute a quick regime proxy here.
    _ema5_quick  = _ema(df["close"].values, 5)
    _ema13_quick = _ema(df["close"].values, 13)
    _regime_quick = "RANGE"
    _ema_spread_q = abs(_ema5_quick - _ema13_quick) / (price + 1e-10) * 100
    # 0.015% spread ≈ 1-2 typical pip moves — real impulses exceed this;
    # flat chop stays well below → safe to separate RANGE from TRENDING
    if _ema_spread_q > 0.015:
        _regime_quick = "TRENDING"

    guarded: list[dict] = []
    for cand in valid:
        if cand["name"] != "impulse_pullback":
            guarded.append(cand)
            continue

        # ── Countertrend: only allowed in TRENDING + 2m expiry ─────────────
        is_countertrend = cand.get("pattern_debug", {}).get("countertrend", False)
        if is_countertrend and expiry == "1m":
            reason = (
                f"impulse_pullback {cand['direction']} REJECTED: "
                f"countertrend setup not allowed on 1m expiry"
            )
            filter_log.append(reason)
            logger.debug(reason)
            continue

        # ── RANGE restriction ───────────────────────────────────────────────
        # Soft penalty: -10pts. Strong IP signals (raw 86+) still survive.
        # Hard reject blocked too many pairs — all sideways markets went silent.
        if _regime_quick == "RANGE":
            _range_penalty = 10.0
            cand["score"] = max(0.0, cand["score"] - _range_penalty)
            cand["filter_penalty"] = cand.get("filter_penalty", 0.0) + _range_penalty
            cand.setdefault("filter_reasons", []).append(
                f"range_penalty: RANGE regime EMA_spread={_ema_spread_q:.4f}% "
                f"(-{_range_penalty:.0f}pts, WR=45.8%)"
            )
            logger.debug(
                "impulse_pullback %s: RANGE soft penalty -%d → score=%.1f",
                cand["direction"], _range_penalty, cand["score"],
            )

        # ── Borderline 1m confirm quality check ────────────────────────────
        # IP borderline setups (score in 68-78) must have a solid confirmation
        # candle. Weak-confirm borderline trades are too marginal for 1m.
        if expiry == "1m":
            raw_score   = cand["score"]
            conf_ratio  = cand.get("pattern_debug", {}).get("conf_body_ratio", 1.0)
            # Widened: more IP trades now require strong confirmation (was 78)
            _IP_BORDERLINE_MAX  = 82.0   # below this = borderline zone (was 78)
            _IP_CONF_RATIO_1M   = 0.55   # need >= 55% of avg_body for borderline 1m
            if raw_score < _IP_BORDERLINE_MAX and conf_ratio < _IP_CONF_RATIO_1M:
                reason = (
                    f"impulse_pullback {cand['direction']} REJECTED: "
                    f"borderline 1m score={raw_score:.1f} conf_ratio={conf_ratio:.2f} "
                    f"< {_IP_CONF_RATIO_1M} — weak confirmation, prefer NO_SIGNAL"
                )
                filter_log.append(reason)
                logger.debug(reason)
                continue

        guarded.append(cand)

    valid = guarded

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

    # ── 6b. Noisy market + weak score = reject ────────────────────────────────
    # Data: noisy=1 has 50.7% WR; only strong signals survive noise.
    _noisy_score_floor = 70.0
    post_noise: list[dict] = []
    for cand in expiry_filtered:
        is_noisy = any("noisy" in r for r in (cand.get("filter_reasons") or []))
        if is_noisy and cand["final_score"] < _noisy_score_floor:
            reason = (
                f"{cand['name']} {cand['direction']} REJECTED: "
                f"noisy structure + score={cand['final_score']:.1f} < {_noisy_score_floor:.0f}"
            )
            filter_log.append(reason)
            logger.debug(reason)
            continue
        post_noise.append(cand)
    expiry_filtered = post_noise

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

    # ── 7a. Level rejection priority edge ────────────────────────────────────
    # If LR and IP are both present and scores are close, give LR a small
    # tie-break bonus when its level is fresh or well-tested.
    # Rationale: LR has explicit level validation (approach + wick + confirm);
    #            IP can win on pattern alone — LR wins ties when both are valid.
    # v4: further widened — LR still underrepresented vs IP despite v3 changes.
    _LR_TIE_WINDOW  = 10.0  # pts: LR must be within this of best IP to get bonus (was 8)
    _LR_TIE_BONUS   = 7.0   # pts added to LR final_score when conditions met (was 5)

    lr_cands = [c for c in expiry_filtered if c["name"] == "level_rejection"]
    ip_cands = [c for c in expiry_filtered if c["name"] == "impulse_pullback"]

    if lr_cands and ip_cands:
        best_lr_now = max(lr_cands, key=lambda c: c["final_score"])
        best_ip_now = max(ip_cands, key=lambda c: c["final_score"])
        gap = best_ip_now["final_score"] - best_lr_now["final_score"]
        if 0.0 <= gap <= _LR_TIE_WINDOW:
            # Check if LR level is fresh or well-tested
            lr_dir     = best_lr_now["direction"].lower()
            lr_dir_dbg = best_lr_now.get("pattern_debug", {}).get(lr_dir, {})
            lr_strong  = lr_dir_dbg.get("is_fresh", False) or lr_dir_dbg.get("touches", 0) >= 2
            if lr_strong:
                best_lr_now["final_score"] = round(
                    best_lr_now["final_score"] + _LR_TIE_BONUS, 1
                )
                best_lr_now.setdefault("filter_reasons", []).append(
                    f"lr_priority_bonus: +{_LR_TIE_BONUS}pts "
                    f"(gap={gap:.1f}pts vs IP, level fresh/strong)"
                )
                logger.debug(
                    "LR priority boost applied: LR %.1f → %.1f (IP was %.1f, gap=%.1f)",
                    best_lr_now["final_score"] - _LR_TIE_BONUS,
                    best_lr_now["final_score"],
                    best_ip_now["final_score"],
                    gap,
                )

    best = max(expiry_filtered, key=lambda c: c["final_score"])
    score = best["final_score"]

    # Score gap: winner vs second-best (shows how dominant the winner is)
    others = sorted(
        [c["final_score"] for c in expiry_filtered if c is not best],
        reverse=True,
    )
    score_gap = round(score - others[0], 1) if others else None

    # ── 8. Per-strategy threshold check ──────────────────────────────────────
    _thresholds = _STRATEGY_THRESHOLDS.get(
        best["name"], {"1m": 68.0, "2m": 65.0}
    )
    min_score = _thresholds.get(expiry, 68.0)

    if score < min_score:
        return _no_signal(
            f"score_below_threshold: {score:.1f} < {min_score:.1f} "
            f"(pattern={best['name']}, expiry={expiry})",
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
                "n_1m":           n_bars_1m,
                "n_5m":           n_bars_5m,
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

    # Pattern-specific debug fields (surfaced for easy log reading)
    _pat_dbg = _extract_pattern_debug(best)

    debug_out = {
        "engine":        "v5_pattern_first",
        "expiry":        expiry,
        "direction":     direction,
        "best_pattern":  best["name"],
        "final_score":   score,
        "raw_score":     best["score"],
        "ctx_score":     best.get("ctx_score", 0.0),
        "min_score":     min_score,
        "min_score_threshold_used": (
            f"{best['name']} {expiry}={min_score} "
            f"(thresholds: IP 1m={_STRATEGY_THRESHOLDS['impulse_pullback']['1m']} "
            f"LR/FB 1m={_STRATEGY_THRESHOLDS['level_rejection']['1m']})"
        ),
        "pattern_q":     best.get("pattern_quality", 0.0),
        "level_q":       best.get("level_quality", 0.0),
        "filter_penalty": best.get("filter_penalty", 0.0),
        "filter_reasons": best.get("filter_reasons", []),
        "score_gap":     score_gap,
        "all_patterns":  [_cand_summary(c) for c in expiry_filtered],
        "filter_log":    filter_log,
        "levels":        levels_debug,
        "context":       ctx,
        "pattern_debug": best.get("pattern_debug", {}),
        "pattern_detail": _pat_dbg,          # flattened, easy to read in logs
        "n_detected":    len(candidates),    # how many patterns were detected
        "n_passed_filter": len(valid),       # after global filters
        "n_passed_expiry": len(expiry_filtered),  # after expiry fit check
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


def _extract_pattern_debug(best: dict) -> dict:
    """
    Flatten pattern-specific debug fields for easy log reading.
    Surfaces the most useful fields per pattern without duplicating full debug.
    """
    name    = best.get("name", "")
    pdbg    = best.get("pattern_debug", {})
    result: dict = {"pattern": name}

    if name == "impulse_pullback":
        result.update({
            "imp_bars":        pdbg.get("imp_bars"),
            "pb_bars":         pdbg.get("pb_bars"),
            "retracement_pct": pdbg.get("retracement_pct"),
            "bars_ago":        pdbg.get("bars_ago"),
            "conf_body_ratio": pdbg.get("conf_body_ratio"),
            "impulse_q":       pdbg.get("impulse_q"),
            "pullback_q":      pdbg.get("pullback_q"),
            "recency":         pdbg.get("recency"),
            "direction_gap":   pdbg.get("direction_gap"),
            "countertrend":    pdbg.get("countertrend"),
            "score_formula":   pdbg.get("score_formula"),
        })
    elif name == "level_rejection":
        direction = best.get("direction", "BUY").lower()
        dir_dbg   = pdbg.get(direction, {})
        result.update({
            "level_price":   dir_dbg.get("level_price"),
            "level_touches": dir_dbg.get("touches"),
            "level_fresh":   dir_dbg.get("is_fresh"),
            "wick_ratio":    dir_dbg.get("rej_wick_ratio"),
            "rej_q":         dir_dbg.get("rej_q"),
            "conf_q":        dir_dbg.get("conf_q"),
            "room_pct":      dir_dbg.get("room_pct"),
        })
    elif name == "false_breakout":
        result.update({
            "tolerance_pct": pdbg.get("tolerance_pct"),
            "breakout_type": pdbg.get("breakout_type"),
        })
    elif name == "compression_breakout":
        result.update({
            "compression_q":   pdbg.get("compression_q"),
            "expansion_ratio": pdbg.get("expansion_ratio"),
            "shadow_pen":      pdbg.get("shadow_pen"),
            "breakout_dist":   pdbg.get("breakout_dist"),
        })

    # Remove None values
    return {k: v for k, v in result.items() if v is not None}


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
            "  %s  PATTERN %-22s %s score=%.1f (pat_q=%.0f lv_q=%.0f ctx=%.0f penalty=%.0f)",
            marker, cand["name"], cand["direction"],
            cand.get("final_score", cand.get("score", 0)),
            cand.get("pattern_quality", 0), cand.get("level_quality", 0),
            cand.get("ctx_score", 0), cand.get("filter_penalty", 0),
        )

    # Pattern-specific detail for winning pattern
    pat_detail = _extract_pattern_debug(best)
    name = best.get("name", "")
    if name == "impulse_pullback":
        logger.info(
            "  ►  IP DETAIL: imp=%s pb=%s ret=%.1f%% bars_ago=%s conf_ratio=%s "
            "gap=%.1f countertrend=%s",
            pat_detail.get("imp_bars"), pat_detail.get("pb_bars"),
            pat_detail.get("retracement_pct", 0),
            pat_detail.get("bars_ago"), pat_detail.get("conf_body_ratio"),
            pat_detail.get("direction_gap", 0), pat_detail.get("countertrend"),
        )
        if pat_detail.get("score_formula"):
            logger.info("  │  IP FORMULA: %s", pat_detail["score_formula"])
    elif name == "level_rejection":
        logger.info(
            "  ►  LR DETAIL: level=%.5f touches=%s fresh=%s wick_ratio=%s "
            "rej_q=%.0f conf_q=%.0f room=%.4f%%",
            pat_detail.get("level_price", 0),
            pat_detail.get("level_touches"), pat_detail.get("level_fresh"),
            pat_detail.get("wick_ratio"),
            pat_detail.get("rej_q", 0), pat_detail.get("conf_q", 0),
            pat_detail.get("room_pct", 0),
        )
    elif name == "compression_breakout":
        logger.info(
            "  ►  CB DETAIL: comp_q=%.1f expand_ratio=%.2f shadow_pen=%.1f dist=%.4f%%",
            pat_detail.get("compression_q", 0), pat_detail.get("expansion_ratio", 0),
            pat_detail.get("shadow_pen", 0), pat_detail.get("breakout_dist", 0),
        )

    # Filter reasons for winning pattern
    for fr in best.get("filter_reasons", []):
        logger.info("  │  WIN FILTER: %s", fr)

    for rej in filter_log:
        logger.info("  │  REJECT: %s", rej)
    logger.info(
        "  └─ RESULT %s score=%.1f stars=%d | detected=%d passed_filter=%d",
        best["direction"], score, _to_stars(score),
        len(all_valid) + len(filter_log),  # approximate
        len(all_valid),
    )
