"""
Signal Scoring Engine — v6

HARD REJECT → NO_SIGNAL:
  - chaotic_noise (regime)
  - low market quality pre-filter (dirty candles, no structure)
  - no primary pattern (both sides < PARTIAL_MATCH_MIN after regime-adj)
  - impulse directly against strong trend
  - BUY near strong resistance / SELL near strong support
  - BUY/SELL parity gap < 5

SOFT PENALTY → lowers confidence:
  - partial match only (no full match): -8
  - weak_trend: -6
  - counter-trend entry: -10
  - moderate proximity to opposite level: -5
  - neutral candles: -5
  - compressed volatility: -4
  - neutral indicators: -3
  - regime-pattern mismatch (non-preferred pattern wins): -6

SIGNAL LEVELS:
  - confidence >= 70 → strong
  - confidence >= 56 → normal
  - confidence <  56 → NO_SIGNAL

BUY/SELL PARITY:
  - gap < 5  → hard reject
  - gap 5-8  → allow at most "normal" quality
  - gap > 8  → allow strong or normal

REGIME-BASED PATTERN PRIORITY:
  - uptrend/downtrend: impulse +15, bounce/breakout -10 (if score < 70)
  - range: bounce/breakout +15, impulse -10 (if score < FULL_MATCH_MIN)
  - weak_trend: no boost, mismatch gets soft penalty
  - chaotic_noise: always NO_SIGNAL

FORMULA:
  confidence = pa_score*0.58 + candle*0.15 + regime*0.10 + level*0.12 + indicator*0.05

RECOMMENDED EXPIRATION:
  - level_bounce / false_breakout → 1m
  - impulse in clear trend         → 2m
  - weak setup / range             → 1m
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
PARTIAL_MATCH_MIN = 30.0   # raw pattern score → "partial match"
STRONG_THRESHOLD  = 70.0   # final confidence → strong
NORMAL_THRESHOLD  = 56.0   # final confidence → normal (was 52 / "moderate")

# Level proximity (buy/sell score from level_analysis)
LEVEL_HARD_BLOCK  = 8      # score < 8 → hard reject (< 0.02% distance)
LEVEL_MEDIUM      = 38     # score < 38 → soft penalty (< 0.15% distance)

# Market quality pre-filter
MARKET_QUALITY_MIN = 30.0  # below this → hard reject

# Counter-trend minimum (bounce/breakout only)
COUNTER_TREND_MIN = 55.0


def run_scoring_engine(df: pd.DataFrame) -> dict[str, Any]:
    n = len(df)
    if n < 10:
        return _no_signal(
            "Недостаточно данных",
            regime="unknown",
            debug={"candles_count": n, "reject_reason": "too_few_candles"}
        )

    # ── 1. Run market regime & candle modules first (used in pre-filter) ───────
    regime  = market_regime_analysis(df)
    candles = candle_strength_analysis(df)

    # ── 2. HARD REJECT: chaotic noise ─────────────────────────────────────────
    if regime.regime == "chaotic_noise":
        debug = {"candles_count": n, "regime": regime.regime,
                 "reject_reason": "chaotic_noise"}
        return _no_signal(f"Хаотичный рынок: {regime.explanation}",
                          regime=regime.regime, debug=debug,
                          hard_conflicts=["Хаотичный рынок"])

    # ── 3. HARD REJECT: market quality pre-filter ──────────────────────────────
    mq_score, mq_reason = _market_quality_check(df)
    if mq_score < MARKET_QUALITY_MIN:
        debug = {"candles_count": n, "regime": regime.regime,
                 "market_quality": round(mq_score, 1),
                 "reject_reason": "low_market_quality"}
        return _no_signal(f"Низкое качество рынка: {mq_reason}",
                          regime=regime.regime, debug=debug,
                          hard_conflicts=[f"Низкое качество рынка ({mq_score:.0f})"])

    # ── 4. Run remaining modules ───────────────────────────────────────────────
    levels     = level_analysis(df)
    indicators = indicator_confirmation(df)
    impulse    = impulse_pullback_strategy(df)
    bounce     = level_bounce_strategy(df, levels.supports, levels.resistances)
    fbreak     = false_breakout_strategy(df, levels.supports, levels.resistances)

    # ── 5. Regime-aware primary pattern assessment ─────────────────────────────
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

    # ── 6. Debug skeleton ─────────────────────────────────────────────────────
    debug: dict[str, Any] = {
        "candles_count":     n,
        "regime":            regime.regime,
        "market_quality":    round(mq_score, 1),
        "last_close":        round(float(df["close"].iloc[-1]), 6),
        "regime_boosts_buy": buy_boosts,
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
        "pa_buy":       round(pa_buy, 1),
        "pa_sell":      round(pa_sell, 1),
        "pa_buy_name":  pa_buy_name,
        "pa_sell_name": pa_sell_name,
        "pa_buy_raw":   round(pa_buy_raw, 1),
        "pa_sell_raw":  round(pa_sell_raw, 1),
        "buy_full":     buy_full,
        "sell_full":    sell_full,
        "buy_partial":  buy_partial,
        "sell_partial": sell_partial,
    }

    # ── 7. HARD REJECT: no primary pattern ────────────────────────────────────
    if not buy_partial and not sell_partial:
        debug["reject_reason"] = "no_primary_pattern"
        debug["reject_detail"] = (
            f"buy_raw={pa_buy_raw:.0f}, sell_raw={pa_sell_raw:.0f} "
            f"(min={PARTIAL_MATCH_MIN})"
        )
        return _no_signal(
            f"Нет паттерна (buy={pa_buy_raw:.0f}, sell={pa_sell_raw:.0f})",
            regime=regime.regime, debug=debug,
            hard_conflicts=["Нет primary pattern"]
        )

    # ── 8. Choose direction (trend-aware) ──────────────────────────────────────
    direction = _pick_direction(
        regime.regime, buy_partial, sell_partial, pa_buy, pa_sell
    )
    debug["direction_candidate"] = direction
    is_buy = direction == "BUY"

    # Assign directional scores
    if is_buy:
        pa_score, pa_name, pa_raw = pa_buy, pa_buy_name, pa_buy_raw
        is_full, is_partial       = buy_full, buy_partial
        level_ok, level_opp       = levels.buy_score, levels.sell_score
        candle_s                  = candles.buy_score
        ind_s                     = indicators.buy_score
        regime_s                  = regime.buy_score
        opp_fb                    = fbreak.sell_score
        ct_bounce                 = bounce.buy_score
        ct_breakout               = fbreak.buy_score
        opp_pa_raw                = pa_sell_raw
    else:
        pa_score, pa_name, pa_raw = pa_sell, pa_sell_name, pa_sell_raw
        is_full, is_partial       = sell_full, sell_partial
        level_ok, level_opp       = levels.sell_score, levels.buy_score
        candle_s                  = candles.sell_score
        ind_s                     = indicators.sell_score
        regime_s                  = regime.sell_score
        opp_fb                    = fbreak.buy_score
        ct_bounce                 = bounce.sell_score
        ct_breakout               = fbreak.sell_score
        opp_pa_raw                = pa_buy_raw

    hard_conflicts: list[str] = []
    soft_penalties: list[str] = []

    # ── 9. Counter-trend rules ────────────────────────────────────────────────
    trend_regimes = {"uptrend", "downtrend"}
    counter_trend = (
        (is_buy  and regime.regime == "downtrend") or
        (not is_buy and regime.regime == "uptrend")
    )

    if regime.regime in trend_regimes and counter_trend:
        best_ct = max(ct_bounce, ct_breakout)
        debug["counter_trend_best"] = round(best_ct, 1)

        if pa_name == "impulse":
            hard_conflicts.append(
                f"Импульс против {regime.regime} — запрещено"
            )
        elif best_ct < COUNTER_TREND_MIN:
            hard_conflicts.append(
                f"Контртренд при {regime.regime}: отбой/пробой={best_ct:.0f} < {COUNTER_TREND_MIN:.0f}"
            )

    # Range + weak impulse → hard block
    if regime.regime == "range" and pa_name == "impulse" and not is_full:
        hard_conflicts.append(
            "Боковой рынок + слабый импульс (partial) — недостаточно"
        )

    # ── 10. HARD REJECT: level too close (BUY near resistance / SELL near support)
    if level_ok < LEVEL_HARD_BLOCK:
        hard_conflicts.append(
            f"Цена у {'сопротивления' if is_buy else 'поддержки'} "
            f"(score={level_ok:.0f}): {levels.explanation}"
        )

    # ── 11. HARD REJECT: strong opposite false breakout ───────────────────────
    if opp_fb >= 65:
        hard_conflicts.append(f"Ложный пробой против сигнала ({opp_fb:.0f})")

    # ── 12. BUY/SELL parity check ─────────────────────────────────────────────
    trend_aligned = (
        (is_buy and regime.regime == "uptrend") or
        (not is_buy and regime.regime == "downtrend")
    )
    gap = abs(pa_buy_raw - pa_sell_raw)
    debug["pa_gap"] = round(gap, 1)

    if gap < 5 and buy_partial and sell_partial and not trend_aligned:
        hard_conflicts.append(
            f"BUY/SELL в паритете ({pa_buy_raw:.0f} vs {pa_sell_raw:.0f}, gap={gap:.0f})"
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

    # ── 13. Confidence (updated weights: level 12%, indicator 5%) ─────────────
    confidence = (
        pa_score  * 0.58
        + candle_s * 0.15
        + regime_s * 0.10
        + level_ok * 0.12
        + ind_s    * 0.05
    )
    debug["confidence_base"] = round(confidence, 1)

    # ── 14. Soft penalties ────────────────────────────────────────────────────
    if not is_full:
        confidence -= 8.0
        soft_penalties.append("Только partial match (-8)")

    if regime.regime == "weak_trend":
        confidence -= 6.0
        soft_penalties.append("Слабый тренд (-6)")

    if counter_trend:
        confidence -= 10.0
        soft_penalties.append("Контртрендовый вход (-10)")

    # Regime-pattern mismatch (non-preferred pattern won)
    regime_mismatch = (
        (regime.regime in ("uptrend", "downtrend") and pa_name != "impulse") or
        (regime.regime == "range" and pa_name == "impulse")
    )
    if regime_mismatch:
        confidence -= 6.0
        soft_penalties.append(f"Паттерн '{pa_name}' не оптимален для {regime.regime} (-6)")

    if LEVEL_HARD_BLOCK <= level_ok < LEVEL_MEDIUM:
        confidence -= 5.0
        soft_penalties.append(f"Умеренная близость к уровню (-5)")

    if candles.neutral_flag:
        confidence -= 5.0
        soft_penalties.append("Нейтральные свечи (-5)")

    if regime.volatility_state == "compressed":
        confidence -= 4.0
        soft_penalties.append("Сжатая волатильность (-4)")

    if ind_s < 20:
        confidence -= 3.0
        soft_penalties.append("Нейтральные индикаторы (-3)")

    debug["soft_penalties"]   = soft_penalties
    debug["confidence_final"] = round(confidence, 1)

    # ── 15. Parity soft cap: gap 5-8 → max "normal" even if confidence >= 70 ──
    parity_cap = False
    if 5 <= gap < 8 and not trend_aligned:
        parity_cap = True
        debug["parity_cap"] = f"gap={gap:.0f} → max normal"

    # ── 16. Signal quality decision ───────────────────────────────────────────
    if confidence < NORMAL_THRESHOLD:
        debug["reject_reason"] = f"low_confidence ({confidence:.0f} < {NORMAL_THRESHOLD})"
        return _no_signal(
            f"Слабый сигнал (уверенность {confidence:.0f})",
            regime=regime.regime,
            soft_penalties=soft_penalties,
            debug=debug
        )

    if confidence >= STRONG_THRESHOLD and not parity_cap:
        signal_quality = "strong"
    else:
        signal_quality = "normal"

    # ── 17. Recommended expiration ────────────────────────────────────────────
    if pa_name in ("bounce", "breakout"):
        rec_exp = "1m"
    elif pa_name == "impulse" and regime.regime in ("uptrend", "downtrend"):
        rec_exp = "2m"
    else:
        rec_exp = "1m"

    # ── 18. Human reasons ─────────────────────────────────────────────────────
    reasons: list[str] = []
    if is_buy:
        if impulse.buy_score  >= PARTIAL_MATCH_MIN: reasons.append(f"Бычий импульс: {impulse.explanation}")
        if bounce.buy_score   >= PARTIAL_MATCH_MIN: reasons.append(f"Отбой поддержки: {bounce.explanation}")
        if fbreak.buy_score   >= PARTIAL_MATCH_MIN: reasons.append(f"Ложный пробой вниз: {fbreak.explanation}")
        if candle_s >= 55: reasons.append(f"Свечи: {candles.explanation}")
        if ind_s >= 45: reasons.append(f"Индикаторы: {indicators.explanation}")
    else:
        if impulse.sell_score  >= PARTIAL_MATCH_MIN: reasons.append(f"Медвежий импульс: {impulse.explanation}")
        if bounce.sell_score   >= PARTIAL_MATCH_MIN: reasons.append(f"Отбой сопротивления: {bounce.explanation}")
        if fbreak.sell_score   >= PARTIAL_MATCH_MIN: reasons.append(f"Ложный пробой вверх: {fbreak.explanation}")
        if candle_s >= 55: reasons.append(f"Свечи: {candles.explanation}")
        if ind_s >= 45: reasons.append(f"Индикаторы: {indicators.explanation}")
    reasons.append(f"Режим: {regime.explanation}")

    conf5 = _to_5(confidence)

    logger.info(
        "Signal: %s (%s) conf=%.1f→%d/5 | %s=%s(raw=%.0f) | regime=%s | "
        "gap=%.0f | soft=%d | mq=%.0f",
        direction, signal_quality, confidence, conf5,
        pa_name, "FULL" if is_full else "partial", pa_raw,
        regime.regime, gap, len(soft_penalties), mq_score
    )

    debug["final_decision"] = direction
    debug["reject_reason"]  = None
    debug["signal_quality"] = signal_quality
    debug["recommended_expiration"] = rec_exp

    flat = {k: {"buy": round(v["buy"], 1), "sell": round(v["sell"], 1)}
            for k, v in debug["modules"].items()}

    return {
        "direction":               direction,
        "signal_quality":          signal_quality,
        "confidence":              round(confidence, 1),
        "confidence_5":            conf5,
        "primary_strategy":        pa_name,
        "primary_match_type":      "full" if is_full else "partial",
        "regime":                  regime.regime,
        "recommended_expiration":  rec_exp,
        "reasons":                 reasons,
        "hard_conflicts":          hard_conflicts,
        "soft_penalties":          soft_penalties,
        "reject_reason":           None,
        "filters_passed":          True,
        "module_scores":           flat,
        "debug":                   debug,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _assess_with_regime(
    imp_s: float,
    bnc_s: float,
    fb_s: float,
    regime_str: str,
) -> tuple[float, str, float, bool, bool, dict]:
    """
    Assess primary patterns with regime-based boosts/penalties applied
    before competition. Raw scores are kept for threshold checks.

    Returns:
        (blended_score, best_name, best_raw, full_match, partial_match, boosts_debug)
    """
    adj = {"impulse": imp_s, "bounce": bnc_s, "breakout": fb_s}
    boosts: dict[str, str] = {}

    if regime_str in ("uptrend", "downtrend"):
        # Trend: boost impulse, penalize bounce/breakout unless very strong
        adj["impulse"] = min(100.0, adj["impulse"] + 15.0)
        boosts["impulse"] = "+15 (trend)"
        if bnc_s < 70:
            adj["bounce"] = max(0.0, adj["bounce"] - 10.0)
            boosts["bounce"] = "-10 (trend)"
        if fb_s < 70:
            adj["breakout"] = max(0.0, adj["breakout"] - 10.0)
            boosts["breakout"] = "-10 (trend)"

    elif regime_str == "range":
        # Range: boost bounce/breakout, penalize weak impulse
        adj["bounce"]   = min(100.0, adj["bounce"]   + 15.0)
        adj["breakout"] = min(100.0, adj["breakout"] + 15.0)
        boosts["bounce"]   = "+15 (range)"
        boosts["breakout"] = "+15 (range)"
        if imp_s < FULL_MATCH_MIN:
            adj["impulse"] = max(0.0, adj["impulse"] - 10.0)
            boosts["impulse"] = "-10 (range)"

    # weak_trend: no boosts, but mismatch is caught as soft penalty in main engine

    best_name = max(adj, key=adj.__getitem__)
    best_adj  = adj[best_name]

    # Raw score (pre-adjustment) for hard-threshold checks
    raw = {"impulse": imp_s, "bounce": bnc_s, "breakout": fb_s}
    best_raw = raw[best_name]

    full_match    = best_raw >= FULL_MATCH_MIN
    partial_match = best_raw >= PARTIAL_MATCH_MIN

    # Blended: adjusted best × 80% + raw average × 20%
    raw_avg = (imp_s + bnc_s + fb_s) / 3.0
    blended = best_adj * 0.80 + raw_avg * 0.20

    return blended, best_name, best_raw, full_match, partial_match, boosts


def _pick_direction(
    regime_str: str,
    buy_partial: bool,
    sell_partial: bool,
    pa_buy: float,
    pa_sell: float,
) -> str:
    """Choose signal direction, preferring trend-aligned side first."""
    if regime_str == "uptrend":
        if buy_partial:  return "BUY"
        if sell_partial: return "SELL"
    elif regime_str == "downtrend":
        if sell_partial: return "SELL"
        if buy_partial:  return "BUY"
    else:
        if buy_partial and (not sell_partial or pa_buy >= pa_sell):
            return "BUY"
    return "SELL"


def _market_quality_check(df: pd.DataFrame) -> tuple[float, str]:
    """
    Pre-filter: evaluate raw candle quality before running full analysis.
    Returns (quality_score 0-100, reason_str).
    Score < MARKET_QUALITY_MIN → hard reject.

    Checks:
    - avg body ratio (tiny bodies = indecision / no structure)
    - direction alternation rate (chaotic flip-flopping)
    - bilateral wicks ratio (big wicks both sides = no conviction)
    - compressed range (too little movement = noise only)
    """
    n = len(df)
    lookback = min(10, n)

    op = df["open"].values[-lookback:]
    cl = df["close"].values[-lookback:]
    hi = df["high"].values[-lookback:]
    lo = df["low"].values[-lookback:]

    body_abs    = np.abs(cl - op)
    total_range = hi - lo + 1e-10
    body_ratio  = body_abs / total_range   # 0=doji, 1=full body

    avg_body_ratio = float(np.mean(body_ratio))

    # Direction alternation
    dirs = (cl > op).astype(int)
    changes = int(sum(dirs[i] != dirs[i - 1] for i in range(1, lookback)))
    alt_rate = changes / max(1, lookback - 1)

    # Bilateral wicks: both upper and lower shadows are large relative to body
    avg_body = float(np.mean(body_abs)) or 1e-8
    upper_sh = hi - np.maximum(op, cl)
    lower_sh = np.minimum(op, cl) - lo
    avg_upper = float(np.mean(upper_sh))
    avg_lower = float(np.mean(lower_sh))
    bilateral_wick = (avg_upper > avg_body * 1.2) and (avg_lower > avg_body * 1.2)

    # Score starts at 100, deduct for bad conditions
    score = 100.0
    reasons = []

    # Tiny bodies
    if avg_body_ratio < 0.15:
        deduct = (0.15 - avg_body_ratio) / 0.15 * 40.0  # up to -40
        score -= deduct
        reasons.append(f"Тела свечей малы ({avg_body_ratio:.2f})")

    # High alternation (chaotic flip-flop without being caught by market_regime chaotic_noise)
    if alt_rate > 0.65:
        deduct = (alt_rate - 0.65) / 0.35 * 35.0  # up to -35
        score -= deduct
        reasons.append(f"Хаотичное чередование ({alt_rate:.0%})")

    # Both sides have large wicks → indecision / trap candles
    if bilateral_wick:
        score -= 20.0
        reasons.append("Тени с обеих сторон (нет направления)")

    score = max(0.0, score)
    reason_str = "; ".join(reasons) if reasons else "OK"
    return score, reason_str


def _no_signal(
    reason: str,
    regime: str = "unknown",
    hard_conflicts: list | None = None,
    soft_penalties: list | None = None,
    debug: dict | None = None,
) -> dict:
    logger.info("NO_SIGNAL: %s", reason)
    d = debug or {}
    d.setdefault("final_decision", "none")
    d.setdefault("reject_reason", reason)
    return {
        "direction":               "NO_SIGNAL",
        "signal_quality":          "none",
        "confidence":              0.0,
        "confidence_5":            0,
        "primary_strategy":        None,
        "primary_match_type":      None,
        "regime":                  regime,
        "recommended_expiration":  None,
        "reasons":                 [],
        "hard_conflicts":          hard_conflicts or [],
        "soft_penalties":          soft_penalties or [],
        "reject_reason":           reason,
        "filters_passed":          False,
        "module_scores":           {},
        "debug":                   d,
    }


def _to_5(score: float) -> int:
    """
    Map confidence → 1–5 display scale.
    Any passing signal (≥ NORMAL_THRESHOLD=56) shows at least 2/5.
    """
    if score >= 85: return 5
    if score >= 75: return 4
    if score >= 65: return 3
    if score >= 56: return 2   # normal signal
    return 1                   # below threshold — not shown to users
