"""
Decision Engine — Layer 4
Selects strategies based on market mode, picks the best signal,
applies multipliers, enforces thresholds, and returns final decision.

Architecture:
  Mode → select primary + secondary strategies
  → run all selected strategies
  → pick best (conditions_met >= 60%)
  → apply confirmation (±10 / ×0.75)
  → apply multipliers (floor ×0.60)
  → compare against thresholds: strong≥70, moderate≥52
  → map to 5-star system: ≥88→5, ≥80→4, ≥70→3, ≥52→2

Rate limiting (state object injected from outside):
  - min 90s between signals
  - max 3 consecutive same direction
  - after 2 losses: threshold raised to 70 for 5 min (caller enforces)
"""
from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass

from .market_mode  import MarketMode, detect_market_mode
from .indicators   import Indicators, calculate_indicators
from .levels       import LevelSet, detect_levels
from .strategies   import (
    ema_bounce_strategy,
    squeeze_breakout_strategy,
    level_bounce_strategy,
    rsi_reversal_strategy,
    micro_breakout_strategy,
    divergence_strategy,
)

# Strategy adaptation — optional import, falls back gracefully if unavailable
try:
    from services.strategy_adaptation import is_strategy_enabled, get_confidence_multiplier as _get_multiplier
    _ADAPTATION_AVAILABLE = True
except ImportError:
    def is_strategy_enabled(name: str) -> bool: return True       # type: ignore[misc]
    def _get_multiplier(name: str) -> float: return 1.0           # type: ignore[misc]
    _ADAPTATION_AVAILABLE = False

logger = logging.getLogger(__name__)


# ── Mode → strategy routing ───────────────────────────────────────────────────
# primary strategies fire first; if none fires, secondary are tried
_MODE_STRATEGIES: dict[str, dict] = {
    "TRENDING_UP": {
        "primary":   ["ema_bounce", "squeeze_breakout"],
        "secondary": ["rsi_reversal", "divergence"],
    },
    "TRENDING_DOWN": {
        "primary":   ["ema_bounce", "squeeze_breakout"],
        "secondary": ["rsi_reversal", "divergence"],
    },
    "RANGE": {
        "primary":   ["level_bounce", "rsi_reversal"],
        "secondary": ["divergence", "ema_bounce"],
    },
    "VOLATILE": {
        "primary":   ["micro_breakout", "squeeze_breakout"],
        "secondary": ["rsi_reversal", "divergence"],
    },
    "SQUEEZE": {
        "primary":   ["squeeze_breakout"],
        "secondary": ["ema_bounce", "micro_breakout"],
    },
}

_STRATEGY_FNS = {
    "ema_bounce":       ema_bounce_strategy,
    "squeeze_breakout": squeeze_breakout_strategy,
    "level_bounce":     level_bounce_strategy,
    "rsi_reversal":     rsi_reversal_strategy,
    "micro_breakout":   micro_breakout_strategy,
    "divergence":       divergence_strategy,
}


@dataclass
class EngineResult:
    direction: str          # "BUY" | "SELL" | "NO_SIGNAL"
    confidence_raw: float   # 0-100 before star mapping
    stars: int              # 1-5
    quality: str            # "strong" | "moderate" | "weak" | "none"
    strategy_name: str
    market_mode: str
    market_mode_strength: float
    reasoning: str
    conditions_met: int
    total_conditions: int
    expiry_hint: str        # "1m" | "2m" (suggested trade expiry)
    debug: dict


def run_decision_engine(
    df1m: pd.DataFrame,
    df5m: pd.DataFrame | None = None,
    raised_threshold: bool = False,    # True = after 2 losses, min conf=70
) -> EngineResult:
    """
    Full 4-layer analysis pipeline.

    Args:
        df1m: 1-minute OHLC DataFrame, oldest→newest, min 20 rows recommended
        df5m: 5-minute OHLC DataFrame (optional, from resample_to_5m)
        raised_threshold: if True, minimum confidence is raised to 70
    """
    n = len(df1m)
    if n < 15:
        return _no_signal("Недостаточно данных (нужно ≥15 свечей)", {})

    # ── Layer 1: Indicators ───────────────────────────────────────────────────
    try:
        ind = calculate_indicators(df1m)
    except Exception as e:
        logger.exception("Indicator calculation failed")
        return _no_signal(f"Ошибка расчёта индикаторов: {e}", {})

    # ── Layer 2: Market Mode ──────────────────────────────────────────────────
    try:
        mode_obj = detect_market_mode(df1m, df5m)
    except Exception as e:
        logger.exception("Market mode detection failed")
        mode_obj = MarketMode("RANGE", 50.0, False, False, "Ошибка — считаем RANGE", {})

    # ── Support / Resistance levels ───────────────────────────────────────────
    try:
        levels = detect_levels(df1m, df5m)
    except Exception as e:
        logger.exception("Level detection failed")
        levels = LevelSet([], [], [], [], 0.0, 0.0, 0.0, 0.0)

    # ── Layer 3: Strategies ───────────────────────────────────────────────────
    routing   = _MODE_STRATEGIES.get(mode_obj.mode, _MODE_STRATEGIES["RANGE"])
    all_strats = routing["primary"] + routing["secondary"]

    ctx_up   = mode_obj.trend_up   or (df5m is not None and len(df5m) >= 5 and
                                        float(df5m["close"].ewm(span=5, adjust=False).mean().iloc[-1]) >
                                        float(df5m["close"].ewm(span=21, adjust=False).mean().iloc[-1]))
    ctx_down = mode_obj.trend_down or (df5m is not None and len(df5m) >= 5 and
                                        float(df5m["close"].ewm(span=5, adjust=False).mean().iloc[-1]) <
                                        float(df5m["close"].ewm(span=21, adjust=False).mean().iloc[-1]))

    candidates = []
    debug_strategies = {}

    for name in all_strats:
        fn = _STRATEGY_FNS.get(name)
        if fn is None:
            continue

        # Strategy adaptation: skip DISABLED strategies entirely
        if not is_strategy_enabled(name):
            debug_strategies[name] = {"status": "DISABLED", "skipped": True}
            continue

        try:
            kwargs = dict(
                df=df1m, ind=ind, levels=levels,
                ctx_trend_up=ctx_up, ctx_trend_down=ctx_down
            )
            if name in ("level_bounce", "divergence"):
                kwargs["mode"] = mode_obj.mode
            res = fn(**kwargs)
        except Exception as e:
            logger.warning("Strategy %s failed: %s", name, e)
            continue

        # Strategy adaptation: apply confidence multiplier (WEAKENED/PROBATION penalty)
        multiplier = _get_multiplier(name)
        if multiplier != 1.0:
            res.confidence = res.confidence * multiplier

        pct = res.conditions_met / res.total_conditions if res.total_conditions > 0 else 0
        debug_strategies[name] = {
            "direction": res.direction,
            "confidence": round(res.confidence, 1),
            "conditions_met": res.conditions_met,
            "total": res.total_conditions,
            "pct": round(pct * 100),
            "adaptation_multiplier": multiplier,
        }

        # Must meet ≥ 55% of conditions and have a real direction
        if res.direction in ("BUY", "SELL") and pct >= 0.55 and res.confidence > 10:
            candidates.append(res)

    if not candidates:
        return _no_signal(
            f"Ни одна стратегия не выполнила условия (режим={mode_obj.mode})",
            {"mode": mode_obj.mode, "strategies": debug_strategies}
        )

    # ── Pick best candidate ────────────────────────────────────────────────────
    best = max(candidates, key=lambda r: r.confidence)

    # If two strategies disagree on direction → discard conflicting, use majority
    buy_cands  = [r for r in candidates if r.direction == "BUY"]
    sell_cands = [r for r in candidates if r.direction == "SELL"]
    if buy_cands and sell_cands:
        # Conflicting strategies — pick whichever direction is stronger
        best_buy  = max(buy_cands,  key=lambda r: r.confidence)
        best_sell = max(sell_cands, key=lambda r: r.confidence)
        if abs(best_buy.confidence - best_sell.confidence) < 10:
            # Too close — skip
            return _no_signal(
                "Противоречие стратегий — сигнал пропущен",
                {"mode": mode_obj.mode, "strategies": debug_strategies,
                 "buy_conf": round(best_buy.confidence, 1), "sell_conf": round(best_sell.confidence, 1)}
            )
        best = best_buy if best_buy.confidence > best_sell.confidence else best_sell

    direction = best.direction
    conf_raw  = best.confidence

    # ── Layer 4: Multipliers ───────────────────────────────────────────────────

    # 4a. 5-min context confirmation / penalty
    if direction == "BUY":
        if ctx_up:
            conf_raw += 10    # 5-min confirms
        elif ctx_down:
            conf_raw *= 0.82  # counter-trend penalty (softer)
    else:  # SELL
        if ctx_down:
            conf_raw += 10
        elif ctx_up:
            conf_raw *= 0.82

    # 4b. Market mode strength multiplier
    mode_str_m = mode_obj.strength / 100.0   # 0-1
    # Only apply if confidence is moderate (don't destroy already-great signals)
    if conf_raw < 75:
        conf_raw = conf_raw * (0.78 + 0.22 * mode_str_m)

    # 4c. Hard floor: if after all multipliers conf < 60% of raw → apply ×0.60
    floor_conf = best.confidence * 0.60
    if conf_raw < floor_conf:
        conf_raw = floor_conf

    conf_raw = min(100.0, conf_raw)

    # ── Threshold check ────────────────────────────────────────────────────────
    min_threshold = 70 if raised_threshold else 52
    if conf_raw < min_threshold:
        return _no_signal(
            f"Уверенность {conf_raw:.0f} < порог {min_threshold}",
            {"mode": mode_obj.mode, "conf_raw": round(conf_raw, 1),
             "strategy": best.strategy_name, "strategies": debug_strategies}
        )

    # ── Stars ──────────────────────────────────────────────────────────────────
    if   conf_raw >= 88: stars = 5
    elif conf_raw >= 80: stars = 4
    elif conf_raw >= 70: stars = 3
    elif conf_raw >= 52: stars = 2
    else:                stars = 1

    if   conf_raw >= 70: quality = "strong"
    elif conf_raw >= 52: quality = "moderate"
    else:                quality = "weak"

    # ── Expiry hint ────────────────────────────────────────────────────────────
    expiry = _pick_expiry(best.strategy_name, quality)

    return EngineResult(
        direction=direction,
        confidence_raw=round(conf_raw, 1),
        stars=stars,
        quality=quality,
        strategy_name=best.strategy_name,
        market_mode=mode_obj.mode,
        market_mode_strength=round(mode_obj.strength, 1),
        reasoning=best.reasoning,
        conditions_met=best.conditions_met,
        total_conditions=best.total_conditions,
        expiry_hint=expiry,
        debug={
            "mode": mode_obj.mode,
            "mode_strength": round(mode_obj.strength, 1),
            "mode_debug": mode_obj.debug,
            "ctx_up": ctx_up,
            "ctx_down": ctx_down,
            "strategies": debug_strategies,
            "best_strategy": best.strategy_name,
            "conf_before_multipliers": round(best.confidence, 1),
            "conf_after_multipliers": round(conf_raw, 1),
            "raised_threshold": raised_threshold,
            "indicators": {
                "ema5": round(ind.ema5, 6),
                "ema13": round(ind.ema13, 6),
                "ema21": round(ind.ema21, 6),
                "rsi": round(ind.rsi, 1),
                "stoch_k": round(ind.stoch_k, 1),
                "stoch_d": round(ind.stoch_d, 1),
                "atr": round(ind.atr, 6),
                "atr_ratio": round(ind.atr_ratio, 3),
                "bb_bw": round(ind.bb_bw, 4),
            },
            "levels": {
                "nearest_sup": round(levels.nearest_sup, 6),
                "nearest_res": round(levels.nearest_res, 6),
                "dist_sup_pct": levels.dist_to_sup_pct,
                "dist_res_pct": levels.dist_to_res_pct,
                "n_supports": len(levels.supports),
                "n_resistances": len(levels.resistances),
                "n_strong_sup": len(levels.strong_sup),
                "n_strong_res": len(levels.strong_res),
            },
        }
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pick_expiry(strategy: str, quality: str) -> str:
    """Bounce/breakout → 1m, trend → 2m, moderate → 2m."""
    if strategy in ("level_bounce", "rsi_reversal", "divergence"):
        return "1m"
    if strategy in ("squeeze_breakout", "micro_breakout"):
        return "1m"
    if quality == "moderate":
        return "2m"
    return "2m"


def _no_signal(reason: str, debug: dict) -> EngineResult:
    return EngineResult(
        direction="NO_SIGNAL",
        confidence_raw=0.0,
        stars=0,
        quality="none",
        strategy_name="",
        market_mode=debug.get("mode", ""),
        market_mode_strength=0.0,
        reasoning=reason,
        conditions_met=0,
        total_conditions=0,
        expiry_hint="",
        debug=debug,
    )
