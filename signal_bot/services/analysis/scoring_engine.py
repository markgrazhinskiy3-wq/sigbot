"""
Signal Scoring Engine
Combines all analysis modules into a single weighted decision.

Weights (must sum to 100):
  Price Action strategies  : 50%  (impulse_pullback, level_bounce, false_breakout)
  Candle Strength          : 15%
  Level Analysis           : 15%
  Market Regime            : 10%
  Indicator Confirmation   : 10%
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

# Minimum weighted score (0-100) required to emit a signal
SIGNAL_THRESHOLD = 52.0


def run_scoring_engine(df: pd.DataFrame, debug: bool = False) -> dict[str, Any]:
    """
    Returns:
      {
        "direction":    "BUY" | "SELL" | "NO_SIGNAL",
        "confidence":   0-100,
        "confidence_5": 0-5,
        "reasons":      [...],
        "regime":       str,
        "filters_passed": bool,
        "module_scores": {...},
        "reject_reason": str | None,
      }
    """
    n = len(df)
    if n < 20:
        return _no_signal("Недостаточно данных (нужно минимум 20 свечей)", {})

    # ── Run all modules ───────────────────────────────────────────────────────
    regime  = market_regime_analysis(df)
    levels  = level_analysis(df)
    candles = candle_strength_analysis(df)
    indicators = indicator_confirmation(df)
    impulse = impulse_pullback_strategy(df)
    bounce  = level_bounce_strategy(df, levels.supports, levels.resistances)
    fbreak  = false_breakout_strategy(df, levels.supports, levels.resistances)

    module_scores: dict[str, dict] = {
        "market_regime":    {"buy": regime.buy_score,      "sell": regime.sell_score},
        "level_analysis":   {"buy": levels.buy_score,      "sell": levels.sell_score},
        "candle_strength":  {"buy": candles.buy_score,     "sell": candles.sell_score},
        "indicators":       {"buy": indicators.buy_score,  "sell": indicators.sell_score},
        "impulse_pullback": {"buy": impulse.buy_score,     "sell": impulse.sell_score},
        "level_bounce":     {"buy": bounce.buy_score,      "sell": bounce.sell_score},
        "false_breakout":   {"buy": fbreak.buy_score,      "sell": fbreak.sell_score},
    }

    # ── Hard filters ──────────────────────────────────────────────────────────
    if regime.regime == "chaotic_noise":
        return _no_signal(
            f"Рынок хаотичный — {regime.explanation}",
            module_scores, regime=regime.regime
        )

    if candles.neutral_flag:
        # Don't block completely, but log
        logger.debug("Candle neutral flag set: %s", candles.explanation)

    # ── Weighted buy / sell scores ────────────────────────────────────────────
    # Price action: average of the 3 PA modules (weight 50% total)
    pa_buy  = (impulse.buy_score + bounce.buy_score + fbreak.buy_score) / 3
    pa_sell = (impulse.sell_score + bounce.sell_score + fbreak.sell_score) / 3

    buy_total = (
        pa_buy                  * 0.50
        + candles.buy_score     * 0.15
        + levels.buy_score      * 0.15
        + regime.buy_score      * 0.10
        + indicators.buy_score  * 0.10
    )
    sell_total = (
        pa_sell                  * 0.50
        + candles.sell_score     * 0.15
        + levels.sell_score      * 0.15
        + regime.sell_score      * 0.10
        + indicators.sell_score  * 0.10
    )

    # ── Conflict check ────────────────────────────────────────────────────────
    # If the two sides are very close, don't emit a signal
    gap = abs(buy_total - sell_total)
    if gap < 8.0 and max(buy_total, sell_total) < SIGNAL_THRESHOLD + 10:
        return _no_signal(
            f"Конфликт направлений (BUY={buy_total:.0f} vs SELL={sell_total:.0f})",
            module_scores, regime=regime.regime
        )

    # ── Direction ─────────────────────────────────────────────────────────────
    if buy_total >= sell_total:
        direction  = "BUY"
        confidence = buy_total
    else:
        direction  = "SELL"
        confidence = sell_total

    if confidence < SIGNAL_THRESHOLD:
        return _no_signal(
            f"Уверенность {confidence:.0f} ниже порога {SIGNAL_THRESHOLD:.0f}",
            module_scores, regime=regime.regime
        )

    # ── Collect human-readable reasons ────────────────────────────────────────
    is_buy   = direction == "BUY"
    reasons  = []

    if is_buy:
        if impulse.buy_score >= 40:
            reasons.append(f"Бычий импульс+откат: {impulse.explanation}")
        if bounce.buy_score >= 35:
            reasons.append(f"Отбой от поддержки: {bounce.explanation}")
        if fbreak.buy_score >= 35:
            reasons.append(f"Ложный пробой вниз: {fbreak.explanation}")
        if candles.buy_score >= 40:
            reasons.append(f"Сила свечей: {candles.explanation}")
        if indicators.buy_score >= 50:
            reasons.append(f"Подтверждение индикаторов: {indicators.explanation}")
    else:
        if impulse.sell_score >= 40:
            reasons.append(f"Медвежий импульс+откат: {impulse.explanation}")
        if bounce.sell_score >= 35:
            reasons.append(f"Отбой от сопротивления: {bounce.explanation}")
        if fbreak.sell_score >= 35:
            reasons.append(f"Ложный пробой вверх: {fbreak.explanation}")
        if candles.sell_score >= 40:
            reasons.append(f"Сила свечей: {candles.explanation}")
        if indicators.sell_score >= 50:
            reasons.append(f"Подтверждение индикаторов: {indicators.explanation}")

    reasons.append(f"Режим рынка: {regime.explanation}")
    reasons.append(f"Уровни: {levels.explanation}")

    if not reasons:
        reasons = ["Большинство факторов согласованы"]

    flat_scores = {k: {"buy": round(v["buy"], 1), "sell": round(v["sell"], 1)}
                   for k, v in module_scores.items()}
    flat_scores["weighted_buy"]  = round(buy_total, 1)
    flat_scores["weighted_sell"] = round(sell_total, 1)

    conf5 = _to_5(confidence)

    logger.info(
        "Signal: %s conf=%.1f (5→%d) | BUY=%.1f SELL=%.1f | regime=%s",
        direction, confidence, conf5, buy_total, sell_total, regime.regime
    )

    return {
        "direction":      direction,
        "confidence":     round(confidence, 1),
        "confidence_5":   conf5,
        "reasons":        reasons,
        "regime":         regime.regime,
        "filters_passed": True,
        "module_scores":  flat_scores,
        "reject_reason":  None,
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _no_signal(reason: str, module_scores: dict, regime: str = "unknown") -> dict:
    logger.info("NO_SIGNAL: %s", reason)
    return {
        "direction":      "NO_SIGNAL",
        "confidence":     0.0,
        "confidence_5":   0,
        "reasons":        [],
        "regime":         regime,
        "filters_passed": False,
        "module_scores":  module_scores,
        "reject_reason":  reason,
    }


def _to_5(score: float) -> int:
    """Map 0-100 score to 1-5 display value."""
    if score >= 80:
        return 5
    if score >= 67:
        return 4
    if score >= 55:
        return 3
    if score >= 43:
        return 2
    return 1
