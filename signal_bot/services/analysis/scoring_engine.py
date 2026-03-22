"""
Signal Scoring Engine — v7

HARD BLOCKS (only two):
  1. BUY and SELL pa_raw scores differ by < 3 (true ambiguity — no dominant side)
  2. All three pattern modules score < 30 on the winning side (no pattern support)

SOFT PENALTIES (applied after base confidence):
  - chaotic_noise regime:              −15
  - weak_trend regime:                 −8
  - incomplete pattern (partial only): −8
  - counter-trend (NOT for breakout):  −10
  - price within 0.08% of opp. level: −5
  - neutral/doji dominate last 3 bars: −5
  - ATR compressed < 50% of 20-avg:   −4

SIGNAL LEVELS:
  - confidence ≥ 68 → strong  (strength 3–5 / 5, scaled within 68–100)
  - confidence ≥ 45 → normal  (strength 2 / 5)
  - confidence <  45 → NO_SIGNAL

FORMULA:
  pa_score   = best_pattern × 0.70 + avg_of_three × 0.30
  confidence = pa_score×0.58 + candles×0.15 + regime×0.10 + indicators×0.12 + levels×0.05

REGIME-BASED PATTERN PRIORITY (applied to best_pattern before blending):
  - uptrend/downtrend: impulse +15, bounce/breakout −10 (if < 70)
  - range:             bounce/breakout +15, impulse −10 (if < FULL_MATCH_MIN)
  - weak_trend/chaotic: no boosts

FALSE BREAKOUT: counter-trend penalty does NOT apply (it is inherently counter-directional).

RECOMMENDED EXPIRATION:
  - level_bounce / false_breakout → 1m
  - impulse in clear trend        → 2m
  - other                         → 1m
"""
import logging
import numpy as np
import pandas as pd
from typing import Any

from .market_regime          import market_regime_analysis
from .level_analysis         import level_analysis
from .candle_strength        import candle_strength_analysis
from .indicator_confirmation import indicator_confirmation
from .impulse_pullback       import impulse_pullback_strategy
from .level_bounce           import level_bounce_strategy
from .false_breakout         import false_breakout_strategy

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
FULL_MATCH_MIN    = 50.0   # raw pattern score → "full match"
PARTIAL_MATCH_MIN = 30.0   # raw pattern score → "partial match" (also hard-block floor)
STRONG_THRESHOLD  = 68.0   # final confidence → strong
NORMAL_THRESHOLD  = 45.0   # final confidence → normal (below = NO_SIGNAL)

# Opposite-level proximity penalty threshold (0–100 score from level_analysis)
LEVEL_OPP_PENALTY = 38     # score < this → apply -5 soft penalty


def run_scoring_engine(df: pd.DataFrame) -> dict[str, Any]:
    n = len(df)
    if n < 10:
        return _no_signal(
            "Недостаточно данных",
            regime="unknown",
            debug={"candles_count": n, "reject_reason": "too_few_candles"}
        )

    # ── 1. Run all modules ────────────────────────────────────────────────────
    regime     = market_regime_analysis(df)
    candles    = candle_strength_analysis(df)
    levels     = level_analysis(df)
    indicators = indicator_confirmation(df)
    impulse    = impulse_pullback_strategy(df)
    bounce     = level_bounce_strategy(df, levels.supports, levels.resistances)
    fbreak     = false_breakout_strategy(df, levels.supports, levels.resistances)

    # ── 2. Regime-aware pattern assessment ───────────────────────────────────
    pa_buy,  pa_buy_name,  pa_buy_raw,  buy_full,  buy_partial,  buy_boosts  = \
        _assess_with_regime(
            impulse.buy_score,  bounce.buy_score,  fbreak.buy_score,
            regime.regime
        )
    pa_sell, pa_sell_name, pa_sell_raw, sell_full, sell_partial, sell_boosts = \
        _assess_with_regime(
            impulse.sell_score, bounce.sell_score, fbreak.sell_score,
            regime.regime
        )

    # ── 3. Debug skeleton ─────────────────────────────────────────────────────
    debug: dict[str, Any] = {
        "candles_count":      n,
        "regime":             regime.regime,
        "last_close":         round(float(df["close"].iloc[-1]), 6),
        "regime_boosts_buy":  buy_boosts,
        "regime_boosts_sell": sell_boosts,
        "modules": {
            "impulse_pullback": {
                "buy": round(impulse.buy_score, 1),
                "sell": round(impulse.sell_score, 1),
                "reason": impulse.explanation,
            },
            "level_bounce": {
                "buy": round(bounce.buy_score, 1),
                "sell": round(bounce.sell_score, 1),
                "reason": bounce.explanation,
            },
            "false_breakout": {
                "buy": round(fbreak.buy_score, 1),
                "sell": round(fbreak.sell_score, 1),
                "reason": fbreak.explanation,
            },
            "candle_strength": {
                "buy": round(candles.buy_score, 1),
                "sell": round(candles.sell_score, 1),
                "reason": candles.explanation,
            },
            "level_analysis": {
                "buy": round(levels.buy_score, 1),
                "sell": round(levels.sell_score, 1),
                "reason": levels.explanation,
            },
            "market_regime": {
                "buy": round(regime.buy_score, 1),
                "sell": round(regime.sell_score, 1),
                "reason": regime.explanation,
            },
            "indicators": {
                "buy": round(indicators.buy_score, 1),
                "sell": round(indicators.sell_score, 1),
                "reason": indicators.explanation,
            },
        },
        "pa_buy":        round(pa_buy, 1),
        "pa_sell":       round(pa_sell, 1),
        "pa_buy_name":   pa_buy_name,
        "pa_sell_name":  pa_sell_name,
        "pa_buy_raw":    round(pa_buy_raw, 1),
        "pa_sell_raw":   round(pa_sell_raw, 1),
        "buy_full":      buy_full,
        "sell_full":     sell_full,
        "buy_partial":   buy_partial,
        "sell_partial":  sell_partial,
    }

    # ── 4. HARD BLOCK 1: no pattern support on either side ───────────────────
    if not buy_partial and not sell_partial:
        debug["reject_reason"] = "no_pattern_support"
        debug["reject_detail"] = (
            f"buy_raw={pa_buy_raw:.0f}, sell_raw={pa_sell_raw:.0f} "
            f"(min={PARTIAL_MATCH_MIN})"
        )
        return _no_signal(
            f"Нет паттерна (buy={pa_buy_raw:.0f}, sell={pa_sell_raw:.0f})",
            regime=regime.regime, debug=debug,
            hard_conflicts=["Нет паттерна (все модули < 30)"]
        )

    # ── 5. Choose direction ───────────────────────────────────────────────────
    direction = _pick_direction(
        regime.regime, buy_partial, sell_partial, pa_buy, pa_sell
    )
    debug["direction_candidate"] = direction
    is_buy = direction == "BUY"

    # Assign directional scores
    if is_buy:
        pa_score, pa_name, pa_raw = pa_buy,  pa_buy_name,  pa_buy_raw
        is_full,  is_partial      = buy_full, buy_partial
        level_own                 = levels.buy_score    # BUY distance from support
        level_opp                 = levels.sell_score   # proximity to resistance (opp)
        candle_s                  = candles.buy_score
        ind_s                     = indicators.buy_score
        regime_s                  = regime.buy_score
        opp_pa_raw                = pa_sell_raw
        imp_raw  = impulse.buy_score
        bnc_raw  = bounce.buy_score
        fb_raw   = fbreak.buy_score
    else:
        pa_score, pa_name, pa_raw = pa_sell,  pa_sell_name,  pa_sell_raw
        is_full,  is_partial      = sell_full, sell_partial
        level_own                 = levels.sell_score
        level_opp                 = levels.buy_score
        candle_s                  = candles.sell_score
        ind_s                     = indicators.sell_score
        regime_s                  = regime.sell_score
        opp_pa_raw                = pa_buy_raw
        imp_raw  = impulse.sell_score
        bnc_raw  = bounce.sell_score
        fb_raw   = fbreak.sell_score

    hard_conflicts: list[str] = []
    soft_penalties: list[str] = []

    # ── 6. HARD BLOCK 2: true ambiguity (gap < 3 with both sides active) ─────
    gap = abs(pa_buy_raw - pa_sell_raw)
    debug["pa_gap"] = round(gap, 1)
    if gap < 3 and buy_partial and sell_partial:
        hard_conflicts.append(
            f"Паритет BUY/SELL ({pa_buy_raw:.0f} vs {pa_sell_raw:.0f}, gap={gap:.0f} < 3)"
        )

    debug["hard_conflicts"] = hard_conflicts

    if hard_conflicts:
        debug["reject_reason"] = "hard_conflict"
        return _no_signal(
            "; ".join(hard_conflicts),
            regime=regime.regime,
            hard_conflicts=hard_conflicts,
            debug=debug
        )

    # ── 7. Confidence (v7 weights) ────────────────────────────────────────────
    # level_opp is the opposing-side score: low score = price near opposite level = bad
    confidence = (
        pa_score  * 0.58
        + candle_s * 0.15
        + regime_s * 0.10
        + ind_s    * 0.12
        + level_opp * 0.05
    )
    debug["confidence_base"] = round(confidence, 1)

    # ── 8. Soft penalties ─────────────────────────────────────────────────────

    # chaotic_noise: penalise heavily but do not block
    if regime.regime == "chaotic_noise":
        confidence -= 15.0
        soft_penalties.append("Хаотичный рынок (-15)")

    # weak_trend
    if regime.regime == "weak_trend":
        confidence -= 8.0
        soft_penalties.append("Слабый тренд (-8)")

    # incomplete pattern
    if not is_full:
        confidence -= 8.0
        soft_penalties.append("Частичный паттерн (-8)")

    # counter-trend — skip for false_breakout (inherently counter-directional)
    counter_trend = (
        (is_buy  and regime.regime == "downtrend") or
        (not is_buy and regime.regime == "uptrend")
    )
    if counter_trend and pa_name != "breakout":
        confidence -= 10.0
        soft_penalties.append(f"Контртренд при {regime.regime} (-10)")

    # price within 0.08% of opposite key level
    # level_opp < LEVEL_OPP_PENALTY means price is close to the opposite level
    if level_opp < LEVEL_OPP_PENALTY:
        confidence -= 5.0
        soft_penalties.append(
            f"Близко к {'сопротивлению' if is_buy else 'поддержке'} (opp_level={level_opp:.0f}) (-5)"
        )

    # neutral / doji dominate last 3 bars
    if candles.neutral_flag:
        confidence -= 5.0
        soft_penalties.append("Нейтральные свечи (-5)")

    # ATR compressed below 50% of 20-period average
    if regime.volatility_state == "compressed":
        confidence -= 4.0
        soft_penalties.append("Сжатая волатильность (-4)")

    debug["soft_penalties"]   = soft_penalties
    debug["confidence_final"] = round(confidence, 1)

    # ── 9. Signal quality decision ────────────────────────────────────────────
    if confidence < NORMAL_THRESHOLD:
        debug["reject_reason"] = f"low_confidence ({confidence:.0f} < {NORMAL_THRESHOLD})"
        return _no_signal(
            f"Слабый сигнал (уверенность {confidence:.0f})",
            regime=regime.regime,
            soft_penalties=soft_penalties,
            debug=debug
        )

    signal_quality = "strong" if confidence >= STRONG_THRESHOLD else "normal"

    # ── 10. Recommended expiration ────────────────────────────────────────────
    if pa_name in ("bounce", "breakout"):
        rec_exp = "1m"
    elif pa_name == "impulse" and regime.regime in ("uptrend", "downtrend"):
        rec_exp = "2m"
    else:
        rec_exp = "1m"

    # ── 11. Human reasons ─────────────────────────────────────────────────────
    reasons: list[str] = []
    if is_buy:
        if imp_raw >= PARTIAL_MATCH_MIN: reasons.append(f"Бычий импульс: {impulse.explanation}")
        if bnc_raw >= PARTIAL_MATCH_MIN: reasons.append(f"Отбой поддержки: {bounce.explanation}")
        if fb_raw  >= PARTIAL_MATCH_MIN: reasons.append(f"Ложный пробой вниз: {fbreak.explanation}")
        if candle_s >= 55: reasons.append(f"Свечи: {candles.explanation}")
        if ind_s    >= 45: reasons.append(f"Индикаторы: {indicators.explanation}")
    else:
        if imp_raw >= PARTIAL_MATCH_MIN: reasons.append(f"Медвежий импульс: {impulse.explanation}")
        if bnc_raw >= PARTIAL_MATCH_MIN: reasons.append(f"Отбой сопротивления: {bounce.explanation}")
        if fb_raw  >= PARTIAL_MATCH_MIN: reasons.append(f"Ложный пробой вверх: {fbreak.explanation}")
        if candle_s >= 55: reasons.append(f"Свечи: {candles.explanation}")
        if ind_s    >= 45: reasons.append(f"Индикаторы: {indicators.explanation}")
    reasons.append(f"Режим: {regime.explanation}")

    conf5 = _to_5(confidence)

    logger.info(
        "Signal: %s (%s) conf=%.1f→%d/5 | %s=%s(raw=%.0f) | regime=%s | "
        "gap=%.0f | soft=%d",
        direction, signal_quality, confidence, conf5,
        pa_name, "FULL" if is_full else "partial", pa_raw,
        regime.regime, gap, len(soft_penalties)
    )

    debug["final_decision"]          = direction
    debug["reject_reason"]           = None
    debug["signal_quality"]          = signal_quality
    debug["recommended_expiration"]  = rec_exp

    flat = {k: {"buy": round(v["buy"], 1), "sell": round(v["sell"], 1)}
            for k, v in debug["modules"].items()}

    return {
        "direction":              direction,
        "signal_quality":         signal_quality,
        "confidence":             round(confidence, 1),
        "confidence_5":           conf5,
        "primary_strategy":       pa_name,
        "primary_match_type":     "full" if is_full else "partial",
        "regime":                 regime.regime,
        "recommended_expiration": rec_exp,
        "reasons":                reasons,
        "hard_conflicts":         hard_conflicts,
        "soft_penalties":         soft_penalties,
        "reject_reason":          None,
        "filters_passed":         True,
        "module_scores":          flat,
        "debug":                  debug,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _assess_with_regime(
    imp_s: float,
    bnc_s: float,
    fb_s: float,
    regime_str: str,
) -> tuple[float, str, float, bool, bool, dict]:
    """
    Apply regime-based boosts to individual pattern scores, then blend:
      pa_score = best_adjusted × 0.70 + avg_of_three_raw × 0.30

    Returns:
        (blended_score, best_name, best_raw, full_match, partial_match, boosts_debug)
    """
    adj = {"impulse": imp_s, "bounce": bnc_s, "breakout": fb_s}
    boosts: dict[str, str] = {}

    if regime_str in ("uptrend", "downtrend"):
        adj["impulse"] = min(100.0, adj["impulse"] + 15.0)
        boosts["impulse"] = "+15 (trend)"
        if bnc_s < 70:
            adj["bounce"] = max(0.0, adj["bounce"] - 10.0)
            boosts["bounce"] = "-10 (trend)"
        if fb_s < 70:
            adj["breakout"] = max(0.0, adj["breakout"] - 10.0)
            boosts["breakout"] = "-10 (trend)"

    elif regime_str == "range":
        adj["bounce"]   = min(100.0, adj["bounce"]   + 15.0)
        adj["breakout"] = min(100.0, adj["breakout"] + 15.0)
        boosts["bounce"]   = "+15 (range)"
        boosts["breakout"] = "+15 (range)"
        if imp_s < FULL_MATCH_MIN:
            adj["impulse"] = max(0.0, adj["impulse"] - 10.0)
            boosts["impulse"] = "-10 (range)"

    # weak_trend / chaotic_noise: no boosts

    best_name = max(adj, key=adj.__getitem__)
    best_adj  = adj[best_name]

    raw_scores = {"impulse": imp_s, "bounce": bnc_s, "breakout": fb_s}
    best_raw   = raw_scores[best_name]

    full_match    = best_raw >= FULL_MATCH_MIN
    partial_match = best_raw >= PARTIAL_MATCH_MIN

    # Blend: best (regime-adjusted) × 0.70 + simple average of raw × 0.30
    raw_avg = (imp_s + bnc_s + fb_s) / 3.0
    blended = best_adj * 0.70 + raw_avg * 0.30

    return blended, best_name, best_raw, full_match, partial_match, boosts


def _pick_direction(
    regime_str: str,
    buy_partial: bool,
    sell_partial: bool,
    pa_buy: float,
    pa_sell: float,
) -> str:
    """
    Choose signal direction.
    Trend-aligned side is preferred, but if the opposite side leads by ≥15 points
    it overrides the trend bias — a strong counter-trend pattern wins.
    """
    COUNTER_OVERRIDE = 15.0   # opposite side must beat trend side by this margin

    if regime_str == "uptrend":
        # Counter-trend SELL override: only if SELL score significantly dominates
        if sell_partial and (pa_sell - pa_buy) >= COUNTER_OVERRIDE:
            return "SELL"
        if buy_partial:
            return "BUY"
        if sell_partial:
            return "SELL"

    elif regime_str == "downtrend":
        # Counter-trend BUY override: only if BUY score significantly dominates
        if buy_partial and (pa_buy - pa_sell) >= COUNTER_OVERRIDE:
            return "BUY"
        if sell_partial:
            return "SELL"
        if buy_partial:
            return "BUY"

    else:
        # No clear trend: pick the stronger side
        if buy_partial and sell_partial:
            return "BUY" if pa_buy >= pa_sell else "SELL"
        if buy_partial:
            return "BUY"

    return "SELL"


def _to_5(confidence: float) -> int:
    """
    Map confidence to 1–5 scale:
      ≥ 88 → 5,  ≥ 78 → 4,  ≥ 68 → 3  (strong tier)
      ≥ 45 → 2                           (normal tier)
       < 45 → 1  (should be NO_SIGNAL, kept as fallback)
    """
    if confidence >= 88:  return 5
    if confidence >= 78:  return 4
    if confidence >= 68:  return 3
    if confidence >= 45:  return 2
    return 1


def _no_signal(
    reason: str,
    regime: str = "unknown",
    debug: dict | None = None,
    hard_conflicts: list[str] | None = None,
    soft_penalties: list[str] | None = None,
) -> dict[str, Any]:
    import html
    safe_reason = html.escape(reason)
    logger.info("NO_SIGNAL: %s (regime=%s)", safe_reason, regime)
    return {
        "direction":              "NO_SIGNAL",
        "signal_quality":         "none",
        "confidence":             0.0,
        "confidence_5":           0,
        "primary_strategy":       None,
        "primary_match_type":     None,
        "regime":                 regime,
        "recommended_expiration": "1m",
        "reasons":                [],
        "hard_conflicts":         hard_conflicts or [],
        "soft_penalties":         soft_penalties or [],
        "reject_reason":          reason,
        "filters_passed":         False,
        "module_scores":          {},
        "debug":                  debug or {"reject_reason": reason},
    }
