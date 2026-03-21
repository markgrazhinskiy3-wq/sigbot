"""
Signal Scoring Engine — v2 (balanced)

Key philosophy:
  - ONE strong primary strategy is enough to generate a signal
  - Secondary filters adjust confidence up/down but rarely block outright
  - Hard conflicts block; soft conflicts penalise
  - "prefer no-signal over noise" but avoid over-blocking

Weights:
  Price Action (best primary + corroboration) : 55%
  Candle Strength                              : 15%
  Level Analysis                               : 10%
  Market Regime                                : 10%
  Indicator Confirmation                       : 10%

Quality thresholds:
  >= 68 → strong signal
  >= 55 → moderate signal
  52-54 + strong primary + no hard conflict → moderate signal (fail-safe)
  < 55  → no signal
"""
import logging
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

STRONG_THRESHOLD   = 68.0
MODERATE_THRESHOLD = 50.0
FAILSAFE_THRESHOLD = 46.0   # only with strong primary + no hard conflict
FAILSAFE_PA_MIN    = 45.0   # primary strategy score needed for fail-safe


def run_scoring_engine(df: pd.DataFrame, debug: bool = False) -> dict[str, Any]:
    n = len(df)
    if n < 20:
        return _no_signal("Недостаточно данных (нужно минимум 20 свечей)", {})

    # ── Run all modules ───────────────────────────────────────────────────────
    regime     = market_regime_analysis(df)
    levels     = level_analysis(df)
    candles    = candle_strength_analysis(df)
    indicators = indicator_confirmation(df)
    impulse    = impulse_pullback_strategy(df)
    bounce     = level_bounce_strategy(df, levels.supports, levels.resistances)
    fbreak     = false_breakout_strategy(df, levels.supports, levels.resistances)

    module_scores: dict[str, dict] = {
        "market_regime":    {"buy": regime.buy_score,      "sell": regime.sell_score},
        "level_analysis":   {"buy": levels.buy_score,      "sell": levels.sell_score},
        "candle_strength":  {"buy": candles.buy_score,     "sell": candles.sell_score},
        "indicators":       {"buy": indicators.buy_score,  "sell": indicators.sell_score},
        "impulse_pullback": {"buy": impulse.buy_score,     "sell": impulse.sell_score},
        "level_bounce":     {"buy": bounce.buy_score,      "sell": bounce.sell_score},
        "false_breakout":   {"buy": fbreak.buy_score,      "sell": fbreak.sell_score},
    }

    # ── HARD FILTER: chaotic market ───────────────────────────────────────────
    if regime.regime == "chaotic_noise":
        return _no_signal(
            f"Хаотичный рынок — {regime.explanation}",
            module_scores, regime=regime.regime
        )

    # ── PA scoring: best primary drives, others add corroboration ────────────
    # For each direction, take the BEST primary strategy (don't average to zero)
    # then blend with mean of all three for corroboration bonus
    def _pa(imp_s: float, bnc_s: float, fb_s: float) -> tuple[float, str, float]:
        """Returns (pa_score, primary_name, primary_raw_score)."""
        scores = {
            "impulse_pullback": imp_s,
            "level_bounce":     bnc_s,
            "false_breakout":   fb_s,
        }
        best_name  = max(scores, key=scores.__getitem__)
        best_score = scores[best_name]
        mean_all   = (imp_s + bnc_s + fb_s) / 3
        # 70% best + 30% mean — so one strong hit dominates
        blended = best_score * 0.70 + mean_all * 0.30
        return blended, best_name, best_score

    pa_buy_score,  pa_buy_name,  pa_buy_raw  = _pa(impulse.buy_score,  bounce.buy_score,  fbreak.buy_score)
    pa_sell_score, pa_sell_name, pa_sell_raw = _pa(impulse.sell_score, bounce.sell_score, fbreak.sell_score)

    buy_total = (
        pa_buy_score            * 0.55
        + candles.buy_score     * 0.15
        + levels.buy_score      * 0.10
        + regime.buy_score      * 0.10
        + indicators.buy_score  * 0.10
    )
    sell_total = (
        pa_sell_score            * 0.55
        + candles.sell_score     * 0.15
        + levels.sell_score      * 0.10
        + regime.sell_score      * 0.10
        + indicators.sell_score  * 0.10
    )

    # ── Direction ─────────────────────────────────────────────────────────────
    if buy_total >= sell_total:
        direction      = "BUY"
        confidence     = buy_total
        primary_name   = pa_buy_name
        primary_raw    = pa_buy_raw
        opp_regime_score = regime.sell_score  # used for hard conflict check
        level_ok_score   = levels.buy_score
        opp_fb_score     = fbreak.sell_score
    else:
        direction      = "SELL"
        confidence     = sell_total
        primary_name   = pa_sell_name
        primary_raw    = pa_sell_raw
        opp_regime_score = regime.buy_score
        level_ok_score   = levels.sell_score
        opp_fb_score     = fbreak.buy_score

    # ── Hard vs soft conflict analysis ───────────────────────────────────────
    hard_conflicts = []
    soft_conflicts = []

    is_buy = direction == "BUY"

    # Hard: regime is strongly opposite
    if opp_regime_score > 70 and (
        (is_buy  and regime.regime == "downtrend") or
        (not is_buy and regime.regime == "uptrend")
    ):
        hard_conflicts.append(
            f"Режим рынка против направления ({regime.regime}): "
            f"{regime.explanation}"
        )

    # Hard: price literally at the opposing level (dist < 0.08%)
    if level_ok_score < 8:
        hard_conflicts.append(
            f"Вход прямо в стену {'BUY→сопротивление' if is_buy else 'SELL→поддержку'}: "
            f"{levels.explanation}"
        )

    # Hard: false breakout confirms opposite direction strongly
    if opp_fb_score >= 50:
        hard_conflicts.append(
            f"Ложный пробой против выбранного направления (score {opp_fb_score:.0f})"
        )

    # Hard: direction gap too small — true conflict
    gap = abs(buy_total - sell_total)
    if gap < 5.0:
        hard_conflicts.append(
            f"Сигналы BUY/SELL почти равны ({buy_total:.0f} vs {sell_total:.0f}) — неопределённость"
        )

    # Soft: neutral indicators
    if indicators.buy_score <= 25 and is_buy:
        soft_conflicts.append("Индикаторы нейтральны для BUY")
    if indicators.sell_score <= 25 and not is_buy:
        soft_conflicts.append("Индикаторы нейтральны для SELL")
    if candles.neutral_flag:
        soft_conflicts.append(f"Свечи неопределённые: {candles.explanation}")
    if level_ok_score < 40:
        soft_conflicts.append(f"Уровень близко: {levels.explanation}")

    # Apply soft conflict penalty to confidence (1.5 pts each — informational, not blocking)
    confidence -= len(soft_conflicts) * 1.5

    # ── Hard conflict → no signal ─────────────────────────────────────────────
    if hard_conflicts:
        reason = "; ".join(hard_conflicts)
        return _no_signal(reason, module_scores, regime=regime.regime,
                          hard_conflicts=hard_conflicts, soft_conflicts=soft_conflicts)

    # ── Signal quality ────────────────────────────────────────────────────────
    if confidence >= STRONG_THRESHOLD:
        signal_quality = "strong"
    elif confidence >= MODERATE_THRESHOLD:
        signal_quality = "moderate"
    elif confidence >= FAILSAFE_THRESHOLD and primary_raw >= FAILSAFE_PA_MIN:
        # Fail-safe: one strong primary strategy, no hard conflict, grey zone
        signal_quality = "moderate"
        logger.debug("Fail-safe signal: primary=%s raw=%.1f conf=%.1f",
                     primary_name, primary_raw, confidence)
    else:
        sc_str = ", ".join(soft_conflicts)
        reason = f"Условия не выполнены (оценка {confidence:.0f})"
        if soft_conflicts:
            reason += f"; конфликты: {sc_str}"
        return _no_signal(
            reason,
            module_scores, regime=regime.regime,
            hard_conflicts=[], soft_conflicts=soft_conflicts
        )

    # ── Collect reasons ───────────────────────────────────────────────────────
    reasons = []
    if is_buy:
        if impulse.buy_score >= 35:
            reasons.append(f"Бычий импульс+откат: {impulse.explanation}")
        if bounce.buy_score >= 30:
            reasons.append(f"Отбой от поддержки: {bounce.explanation}")
        if fbreak.buy_score >= 30:
            reasons.append(f"Ложный пробой вниз: {fbreak.explanation}")
        if candles.buy_score >= 35:
            reasons.append(f"Сила свечей: {candles.explanation}")
        if indicators.buy_score >= 50:
            reasons.append(f"Индикаторы подтверждают: {indicators.explanation}")
    else:
        if impulse.sell_score >= 35:
            reasons.append(f"Медвежий импульс+откат: {impulse.explanation}")
        if bounce.sell_score >= 30:
            reasons.append(f"Отбой от сопротивления: {bounce.explanation}")
        if fbreak.sell_score >= 30:
            reasons.append(f"Ложный пробой вверх: {fbreak.explanation}")
        if candles.sell_score >= 35:
            reasons.append(f"Сила свечей: {candles.explanation}")
        if indicators.sell_score >= 50:
            reasons.append(f"Индикаторы подтверждают: {indicators.explanation}")

    reasons.append(f"Режим: {regime.explanation}")
    if not reasons:
        reasons = ["Основная стратегия подтверждена"]

    # ── Flatten scores for debug ──────────────────────────────────────────────
    flat = {k: {"buy": round(v["buy"], 1), "sell": round(v["sell"], 1)}
            for k, v in module_scores.items()}
    flat["weighted_buy"]  = round(buy_total,  1)
    flat["weighted_sell"] = round(sell_total, 1)

    conf5 = _to_5(confidence)

    logger.info(
        "Signal: %s (%s) conf=%.1f→%d/5 | primary=%s(%.0f) | regime=%s | "
        "soft=%d hard=0",
        direction, signal_quality, confidence, conf5,
        primary_name, primary_raw, regime.regime, len(soft_conflicts)
    )

    return {
        "direction":        direction,
        "signal_quality":   signal_quality,
        "confidence":       round(confidence, 1),
        "confidence_5":     conf5,
        "primary_strategy": primary_name,
        "reasons":          reasons,
        "soft_conflicts":   soft_conflicts,
        "hard_conflicts":   hard_conflicts,
        "regime":           regime.regime,
        "filters_passed":   True,
        "module_scores":    flat,
        "reject_reason":    None,
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _no_signal(
    reason: str,
    module_scores: dict,
    regime: str = "unknown",
    hard_conflicts: list | None = None,
    soft_conflicts: list | None = None,
) -> dict:
    logger.info("NO_SIGNAL: %s", reason)
    return {
        "direction":        "NO_SIGNAL",
        "signal_quality":   "none",
        "confidence":       0.0,
        "confidence_5":     0,
        "primary_strategy": None,
        "reasons":          [],
        "soft_conflicts":   soft_conflicts or [],
        "hard_conflicts":   hard_conflicts or [],
        "regime":           regime,
        "filters_passed":   False,
        "module_scores":    module_scores,
        "reject_reason":    reason,
    }


def _to_5(score: float) -> int:
    if score >= 80: return 5
    if score >= 68: return 4
    if score >= 58: return 3
    if score >= 52: return 2
    return 1
