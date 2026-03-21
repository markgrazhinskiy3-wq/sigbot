"""
Signal Scoring Engine — v3

Decision flow:
  1. Validate & count candles
  2. Run all modules
  3. If chaotic market → NO_SIGNAL
  4. For each direction: best primary strategy score drives
  5. ANY primary strategy score >= MIN_PRIMARY → candidate signal
  6. Check hard conflicts (block outright)
  7. Secondary filters adjust confidence
  8. If adjusted confidence >= MODERATE_THRESHOLD → issue signal
  9. Full debug breakdown always attached

Thresholds (intentionally relaxed):
  MIN_PRIMARY  = 30   — minimum primary pattern score to consider direction
  MODERATE     = 40   — minimum final confidence for moderate signal
  STRONG       = 62   — final confidence for strong signal
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

MIN_PRIMARY      = 30.0
MODERATE_THRESHOLD = 40.0
STRONG_THRESHOLD   = 62.0


def run_scoring_engine(df: pd.DataFrame) -> dict[str, Any]:
    n = len(df)
    if n < 10:
        return _no_signal(
            "Недостаточно данных",
            {}, regime="unknown",
            debug={"candles_count": n, "error": "too_few_candles"}
        )

    # ── 1. Run all modules ────────────────────────────────────────────────────
    regime     = market_regime_analysis(df)
    levels     = level_analysis(df)
    candles    = candle_strength_analysis(df)
    indicators = indicator_confirmation(df)
    impulse    = impulse_pullback_strategy(df)
    bounce     = level_bounce_strategy(df, levels.supports, levels.resistances)
    fbreak     = false_breakout_strategy(df, levels.supports, levels.resistances)

    # ── 2. Build full debug breakdown ─────────────────────────────────────────
    debug = {
        "candles_count": n,
        "regime": regime.regime,
        "last_close": round(float(df["close"].iloc[-1]), 6),
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
    }

    # ── 3. Hard filter: chaotic market ────────────────────────────────────────
    if regime.regime == "chaotic_noise":
        debug["reject_reason"] = "chaotic_noise"
        return _no_signal(f"Хаотичный рынок: {regime.explanation}", {}, regime=regime.regime, debug=debug)

    # ── 4. Primary pattern scoring: best-of-three per direction ───────────────
    # One strong pattern is enough — don't average into zero
    def _best_primary(imp, bnc, fb) -> tuple[float, str]:
        candidates = {"impulse": imp, "bounce": bnc, "breakout": fb}
        best_name  = max(candidates, key=candidates.__getitem__)
        best_score = candidates[best_name]
        # Blended: best drives 70%, average adds 30%
        avg = (imp + bnc + fb) / 3
        blended = best_score * 0.70 + avg * 0.30
        return blended, best_name

    pa_buy,  pa_buy_name  = _best_primary(impulse.buy_score,  bounce.buy_score,  fbreak.buy_score)
    pa_sell, pa_sell_name = _best_primary(impulse.sell_score, bounce.sell_score, fbreak.sell_score)

    debug["pa_buy"]       = round(pa_buy, 1)
    debug["pa_sell"]      = round(pa_sell, 1)
    debug["pa_buy_name"]  = pa_buy_name
    debug["pa_sell_name"] = pa_sell_name

    # ── 5. Check if any primary pattern fires ─────────────────────────────────
    raw_buy_primary  = max(impulse.buy_score,  bounce.buy_score,  fbreak.buy_score)
    raw_sell_primary = max(impulse.sell_score, bounce.sell_score, fbreak.sell_score)

    has_buy_signal  = raw_buy_primary  >= MIN_PRIMARY
    has_sell_signal = raw_sell_primary >= MIN_PRIMARY

    debug["raw_primary_buy"]  = round(raw_buy_primary, 1)
    debug["raw_primary_sell"] = round(raw_sell_primary, 1)
    debug["has_buy_signal"]   = has_buy_signal
    debug["has_sell_signal"]  = has_sell_signal

    if not has_buy_signal and not has_sell_signal:
        # No primary pattern at all — check fallback via secondary signals
        # If regime + candles strongly agree, still issue a signal
        fallback_buy  = (regime.buy_score  * 0.5 + candles.buy_score  * 0.5)
        fallback_sell = (regime.sell_score * 0.5 + candles.sell_score * 0.5)
        debug["fallback_buy"]  = round(fallback_buy, 1)
        debug["fallback_sell"] = round(fallback_sell, 1)

        if fallback_buy >= 60 and fallback_buy > fallback_sell * 1.1:
            # Use fallback as a very weak primary
            pa_buy = fallback_buy * 0.7
            has_buy_signal = True
            debug["fallback_used"] = "buy"
        elif fallback_sell >= 60 and fallback_sell > fallback_buy * 1.1:
            pa_sell = fallback_sell * 0.7
            has_sell_signal = True
            debug["fallback_used"] = "sell"
        else:
            debug["reject_reason"] = "no_primary_pattern"
            debug["reject_detail"] = (
                f"PA buy={raw_buy_primary:.0f}, sell={raw_sell_primary:.0f} "
                f"(min={MIN_PRIMARY}); fallback buy={fallback_buy:.0f}, sell={fallback_sell:.0f}"
            )
            return _no_signal(
                f"Нет паттерна (PA buy={raw_buy_primary:.0f}, sell={raw_sell_primary:.0f})",
                {}, regime=regime.regime, debug=debug
            )

    # ── 6. Direction: highest PA score wins ───────────────────────────────────
    if has_buy_signal and (not has_sell_signal or pa_buy >= pa_sell):
        direction    = "BUY"
        confidence_base = (
            pa_buy                 * 0.50
            + candles.buy_score   * 0.20
            + levels.buy_score    * 0.10
            + regime.buy_score    * 0.10
            + indicators.buy_score * 0.10
        )
        primary_name = pa_buy_name
        primary_raw  = raw_buy_primary
        opp_regime   = regime.sell_score
        level_ok     = levels.buy_score
        opp_fb       = fbreak.sell_score
    else:
        direction    = "SELL"
        confidence_base = (
            pa_sell                 * 0.50
            + candles.sell_score   * 0.20
            + levels.sell_score    * 0.10
            + regime.sell_score    * 0.10
            + indicators.sell_score * 0.10
        )
        primary_name = pa_sell_name
        primary_raw  = raw_sell_primary
        opp_regime   = regime.buy_score
        level_ok     = levels.sell_score
        opp_fb       = fbreak.buy_score

    is_buy = direction == "BUY"
    debug["direction_candidate"] = direction
    debug["confidence_base"]     = round(confidence_base, 1)

    # ── 7. Hard conflicts ─────────────────────────────────────────────────────
    hard_conflicts = []

    # Strongly opposite regime (score > 75)
    if opp_regime > 75 and (
        (is_buy  and regime.regime == "downtrend") or
        (not is_buy and regime.regime == "uptrend")
    ):
        hard_conflicts.append(f"Режим рынка противоположный ({regime.regime})")

    # Price literally at the wall (dist < 0.02%)
    if level_ok < 8:
        hard_conflicts.append(f"Цена у самого {'сопротивления' if is_buy else 'поддержки'}: {levels.explanation}")

    # False breakout strongly confirms opposite direction
    if opp_fb >= 55:
        hard_conflicts.append(f"Ложный пробой против сигнала (score={opp_fb:.0f})")

    # Buy and sell are too close (gap < 3 pts in primary raw)
    gap = abs(raw_buy_primary - raw_sell_primary)
    if gap < 3.0 and has_buy_signal and has_sell_signal:
        hard_conflicts.append(f"BUY/SELL в паритете ({raw_buy_primary:.0f} vs {raw_sell_primary:.0f})")

    debug["hard_conflicts"] = hard_conflicts

    if hard_conflicts:
        debug["reject_reason"] = "hard_conflict"
        return _no_signal(
            "; ".join(hard_conflicts), {},
            regime=regime.regime,
            hard_conflicts=hard_conflicts, soft_conflicts=[],
            debug=debug
        )

    # ── 8. Soft conflicts ─────────────────────────────────────────────────────
    soft_conflicts = []
    if candles.neutral_flag:
        soft_conflicts.append("Свечи неопределённые")
    if level_ok < 40:
        soft_conflicts.append(f"Уровень близко ({levels.explanation})")
    if (is_buy  and indicators.buy_score  < 20) or \
       (not is_buy and indicators.sell_score < 20):
        soft_conflicts.append("Индикаторы нейтральны")

    confidence = confidence_base - len(soft_conflicts) * 1.5
    debug["soft_conflicts"]  = soft_conflicts
    debug["confidence_final"] = round(confidence, 1)

    # ── 9. Signal quality ─────────────────────────────────────────────────────
    if confidence >= STRONG_THRESHOLD:
        signal_quality = "strong"
    elif confidence >= MODERATE_THRESHOLD:
        signal_quality = "moderate"
    else:
        debug["reject_reason"] = f"low_confidence ({confidence:.0f} < {MODERATE_THRESHOLD})"
        return _no_signal(
            f"Слабый сигнал (уверенность {confidence:.0f})",
            {}, regime=regime.regime,
            hard_conflicts=[], soft_conflicts=soft_conflicts,
            debug=debug
        )

    # ── 10. Build reasons ─────────────────────────────────────────────────────
    reasons = []
    if is_buy:
        if impulse.buy_score  >= 25: reasons.append(f"Бычий импульс: {impulse.explanation}")
        if bounce.buy_score   >= 25: reasons.append(f"Отбой поддержки: {bounce.explanation}")
        if fbreak.buy_score   >= 25: reasons.append(f"Ложный пробой вниз: {fbreak.explanation}")
        if candles.buy_score  >= 35: reasons.append(f"Сила свечей: {candles.explanation}")
        if indicators.buy_score >= 45: reasons.append(f"Индикаторы: {indicators.explanation}")
    else:
        if impulse.sell_score  >= 25: reasons.append(f"Медвежий импульс: {impulse.explanation}")
        if bounce.sell_score   >= 25: reasons.append(f"Отбой сопротивления: {bounce.explanation}")
        if fbreak.sell_score   >= 25: reasons.append(f"Ложный пробой вверх: {fbreak.explanation}")
        if candles.sell_score  >= 35: reasons.append(f"Сила свечей: {candles.explanation}")
        if indicators.sell_score >= 45: reasons.append(f"Индикаторы: {indicators.explanation}")

    reasons.append(f"Режим: {regime.explanation}")
    if not reasons:
        reasons = ["Основная стратегия подтверждена"]

    conf5 = _to_5(confidence)

    logger.info(
        "Signal: %s (%s) conf=%.1f→%d/5 | primary=%s(%.0f) | regime=%s | soft=%d",
        direction, signal_quality, confidence, conf5,
        primary_name, primary_raw, regime.regime, len(soft_conflicts)
    )

    flat = {k: {"buy": round(v["buy"], 1), "sell": round(v["sell"], 1)}
            for k, v in debug["modules"].items()}
    flat["weighted_buy"]  = round(confidence_base if is_buy  else 0, 1)
    flat["weighted_sell"] = round(confidence_base if not is_buy else 0, 1)

    debug["final_decision"] = direction
    debug["reject_reason"]  = None

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
        "debug":            debug,
    }


def _no_signal(
    reason: str,
    module_scores: dict,
    regime: str = "unknown",
    hard_conflicts: list | None = None,
    soft_conflicts: list | None = None,
    debug: dict | None = None,
) -> dict:
    logger.info("NO_SIGNAL: %s", reason)
    d = debug or {}
    d.setdefault("final_decision", "none")
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
        "debug":            d,
    }


def _to_5(score: float) -> int:
    if score >= 78: return 5
    if score >= 62: return 4
    if score >= 52: return 3
    if score >= 42: return 2
    return 1
