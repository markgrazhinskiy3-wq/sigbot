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

import asyncio

from .market_mode  import MarketMode, detect_market_mode
from .indicators   import Indicators, calculate_indicators
from .levels       import LevelSet, detect_levels
from .strategies   import (
    ema_bounce_strategy,
    level_breakout_strategy,
    level_touch_strategy,
    rsi_bb_scalp_strategy,
    three_candle_reversal_strategy,
    stoch_snap_strategy,
    ema_micro_cross_strategy,
    otc_trend_confirm_strategy,
    double_bottom_top_strategy,
)

# Strategy adaptation — optional import, falls back gracefully if unavailable
try:
    from services.strategy_adaptation import is_strategy_enabled
except ImportError:
    def is_strategy_enabled(name: str) -> bool: return True       # type: ignore[misc]

logger = logging.getLogger(__name__)


# ── Mode → strategy routing ───────────────────────────────────────────────────
# New strategies (1-6 from the trading document) are PRIMARY across all modes.
# Old strategies kept as secondary fallbacks.
#
# 1m strategies: rsi_bb_scalp, three_candle_reversal, stoch_snap, ema_micro_cross
# 2m strategies: otc_trend_confirm, double_bottom_top
_MODE_STRATEGIES: dict[str, dict] = {
    "TRENDING_UP": {
        "priority":  [],
        "primary":   ["ema_micro_cross", "otc_trend_confirm", "rsi_bb_scalp", "three_candle_reversal"],
        "secondary": [],
    },
    "TRENDING_DOWN": {
        "priority":  [],
        "primary":   ["ema_micro_cross", "otc_trend_confirm", "rsi_bb_scalp", "three_candle_reversal"],
        "secondary": [],
    },
    "RANGE": {
        "priority":  [],
        "primary":   ["rsi_bb_scalp", "stoch_snap", "three_candle_reversal", "double_bottom_top"],
        "secondary": ["ema_micro_cross"],
    },
    "VOLATILE": {
        "priority":  [],
        "primary":   ["rsi_bb_scalp", "stoch_snap", "three_candle_reversal"],
        "secondary": [],
    },
    "SQUEEZE": {
        "priority":  [],
        "primary":   ["rsi_bb_scalp", "stoch_snap", "double_bottom_top", "ema_micro_cross"],
        "secondary": [],
    },
}

_STRATEGY_FNS = {
    "level_touch":          level_touch_strategy,
    "ema_bounce":           ema_bounce_strategy,
    "level_breakout":       level_breakout_strategy,
    "rsi_bb_scalp":         rsi_bb_scalp_strategy,
    "three_candle_reversal": three_candle_reversal_strategy,
    "stoch_snap":           stoch_snap_strategy,
    "ema_micro_cross":      ema_micro_cross_strategy,
    "otc_trend_confirm":    otc_trend_confirm_strategy,
    "double_bottom_top":    double_bottom_top_strategy,
}

# Strategies that suggest 1-minute expiry
_ONE_MIN_STRATEGIES = {"rsi_bb_scalp", "stoch_snap", "three_candle_reversal", "ema_micro_cross"}

# Strategies that suggest 2-minute expiry
_TWO_MIN_STRATEGIES = {"otc_trend_confirm", "double_bottom_top"}


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
    df1m_ctx: pd.DataFrame | None = None,   # 1-min resampled from 15s — middle-tier context
    raised_threshold: bool = False,          # True = after 2 losses, min conf=70
    n_bars_15s: int = 0,
    n_bars_1m:  int = 0,
    n_bars_5m:  int = 0,
    symbol: str = "",                        # OTC symbol e.g. "#AUDCAD_otc" — for pair-specific params
    expiry: str = "both",                    # "1m" | "2m" | "both" — filters strategy pool by expiry tier
) -> EngineResult:
    """
    Full 4-layer analysis pipeline optimised for 1-2 min OTC expiry.

    Args:
        df1m:        1-min resampled OHLC DataFrame (primary analysis), oldest→newest, min 15 rows
                     Falls back to raw 15s OHLC at startup (fewer than 15 one-minute bars).
        df5m:        5-min resampled OHLC (macro context, optional)
        df1m_ctx:    1-min resampled OHLC (same as df1m — used for MTF context inside engine)
        raised_threshold: if True, minimum confidence is raised to 70
        n_bars_15s:  number of raw 15-sec bars (informational, for debug)
        n_bars_1m:   number of 1-min bars after resampling
        n_bars_5m:   number of 5-min bars after resampling
        expiry:      "1m" → run only 1-min strategies; "2m" → run only 2-min strategies;
                     "both" → no filter (auto-scanner and fallback)
    """
    n = len(df1m)
    _bar_debug = {"n_bars_15s": n_bars_15s or n, "n_bars_1m": n_bars_1m, "n_bars_5m": n_bars_5m}

    # ── Pair profile (classification + parameter overrides) ───────────────────
    try:
        from .pair_profile import get_pair_params, get_disabled_strategies
        _pair_params      = get_pair_params(symbol) if symbol else None
        _disabled_strats  = get_disabled_strategies(symbol) if symbol else set()
    except Exception:
        _pair_params     = None
        _disabled_strats = set()

    if n < 15:
        return _no_signal("Недостаточно данных (нужно ≥15 свечей)", {**_bar_debug})

    # ── Layer 1: Indicators ───────────────────────────────────────────────────
    try:
        ind = calculate_indicators(df1m)
    except Exception as e:
        logger.exception("Indicator calculation failed")
        return _no_signal(f"Ошибка расчёта индикаторов: {e}", {**_bar_debug})

    # snapshot of indicator values for NO_SIGNAL debug (computed once, reused)
    _ind_dbg: dict = {
        "indicators": {
            "atr":       round(ind.atr, 6),
            "atr_ratio": round(ind.atr_ratio, 3),
            "rsi":       round(ind.rsi, 1),
            "stoch_k":   round(ind.stoch_k, 1),
            "stoch_d":   round(ind.stoch_d, 1),
            "ema5":      round(ind.ema5, 6),
            "ema13":     round(ind.ema13, 6),
            "ema21":     round(ind.ema21, 6),
            "bb_bw":     round(ind.bb_bw, 5),
            "momentum":  round(ind.momentum, 6),
        }
    }

    # ── DEAD MARKET FILTER 1: avg candle body too small ───────────────────────
    # If the average candle body over the last 20 15s bars is < 0.003% of price,
    # the market is pure noise — no strategy can profitably trade it.
    price_now = float(df1m["close"].iloc[-1])
    _lookback = min(20, n)
    avg_body_pct = float(
        np.mean(np.abs(df1m["close"].values[-_lookback:] - df1m["open"].values[-_lookback:]))
    ) / price_now * 100 if price_now > 0 else 0.0
    _DEAD_BODY_THRESHOLD = 0.003   # 0.003% — below this = noise only
    if avg_body_pct < _DEAD_BODY_THRESHOLD:
        logger.info("DEAD MARKET: avg_body_pct=%.5f%% < %.3f%% — skipping", avg_body_pct, _DEAD_BODY_THRESHOLD)
        return _no_signal(f"dead_market (avg_body={avg_body_pct:.5f}% < {_DEAD_BODY_THRESHOLD}%)", {
            **_bar_debug, **_ind_dbg,
            "avg_body_pct": round(avg_body_pct, 6),
            "dead_market": True,
        })

    # ── Layer 2: Market Mode ──────────────────────────────────────────────────
    try:
        mode_obj = detect_market_mode(df1m, df5m)
    except Exception as e:
        logger.exception("Market mode detection failed")
        mode_obj = MarketMode("RANGE", 50.0, False, False, "Ошибка — считаем RANGE", {})

    # ── DEAD MARKET FILTER 2: fake trending — EMA spread too small ───────────
    # If EMA5 and EMA21 are separated by < 0.002% but mode is TRENDING,
    # it's a flat line with microscopic slope — override to RANGE.
    ema_spread_pct = abs(ind.ema5 - ind.ema21) / price_now * 100 if price_now > 0 else 0.0
    _FAKE_TREND_EMA_THRESHOLD = 0.002   # 0.002% — EMAs effectively flat
    if ema_spread_pct < _FAKE_TREND_EMA_THRESHOLD and mode_obj.mode.startswith("TRENDING"):
        logger.info(
            "FAKE TREND override: ema_spread=%.5f%% < %.3f%% — %s → RANGE(40)",
            ema_spread_pct, _FAKE_TREND_EMA_THRESHOLD, mode_obj.mode,
        )
        mode_obj = MarketMode(
            "RANGE", 40.0, False, False,
            f"EMA spread {ema_spread_pct:.5f}% < {_FAKE_TREND_EMA_THRESHOLD}% — fake trend overridden to RANGE",
            {"original_mode": mode_obj.mode, "ema_spread_pct": round(ema_spread_pct, 6)},
        )

    # ── Support / Resistance levels ───────────────────────────────────────────
    try:
        levels = detect_levels(df1m, df5m)
    except Exception as e:
        logger.exception("Level detection failed")
        levels = LevelSet([], [], [], [], 0.0, 0.0, 0.0, 0.0)

    _lvl_dbg: dict = {
        "levels": {
            "nearest_sup":   round(levels.nearest_sup, 6),
            "nearest_res":   round(levels.nearest_res, 6),
            "dist_sup_pct":  levels.dist_to_sup_pct,
            "dist_res_pct":  levels.dist_to_res_pct,
            "n_supports":    len(levels.supports),
            "n_resistances": len(levels.resistances),
        }
    }

    # ── Layer 3: Strategies ───────────────────────────────────────────────────
    routing = _MODE_STRATEGIES.get(mode_obj.mode, _MODE_STRATEGIES["RANGE"])

    # ── Expiry filter: keep only strategies matching the requested expiry tier ─
    # "1m"  → exclude 2-min strategies from all tiers
    # "2m"  → exclude 1-min strategies from all tiers
    # "both"→ no filter (auto-scanner lets the engine pick expiry naturally)
    if expiry in ("1m", "2m"):
        _exclude = _TWO_MIN_STRATEGIES if expiry == "1m" else _ONE_MIN_STRATEGIES
        routing = {
            tier: [s for s in names if s not in _exclude]
            for tier, names in routing.items()
        }
        logger.info("Expiry filter '%s': excluded %s", expiry, _exclude)

    # ── Multi-timeframe context ───────────────────────────────────────────────
    #
    # Layer A — short-term (1-3 min): EMA(3) vs EMA(8) on resampled 1-min bars
    #   Tells us the direction of the last few minutes.
    #
    # Layer B — macro trend: linear regression slope over ALL available 1-min bars
    #   50 fifteen-second bars → ~13 one-minute bars → slope is reliable without
    #   needing 8+ five-minute bars.  5m EMA(5/21) is skipped because 4 bars
    #   make it essentially noise.
    #
    # Bonus (+7) and penalty (×0.82) only fire when BOTH layers agree.

    # Layer A — 1m EMA direction
    ctx_up_1m = ctx_dn_1m = False
    if df1m_ctx is not None and len(df1m_ctx) >= 5:
        e3 = float(df1m_ctx["close"].ewm(span=3, adjust=False).mean().iloc[-1])
        e8 = float(df1m_ctx["close"].ewm(span=8, adjust=False).mean().iloc[-1])
        ctx_up_1m = e3 > e8
        ctx_dn_1m = e3 < e8

    # Layer B — macro slope (linear regression on 1-min closes)
    ctx_macro_up = ctx_macro_dn = False
    ctx_macro_note = "slope_na"
    if df1m_ctx is not None and len(df1m_ctx) >= 6:
        closes = df1m_ctx["close"].values.astype(float)
        x      = np.arange(len(closes))
        slope  = float(np.polyfit(x, closes, 1)[0])
        norm_slope = slope / float(closes.mean())          # fraction per 1-min bar
        ctx_macro_up = norm_slope >  5e-5                  # +0.005%/bar → upward
        ctx_macro_dn = norm_slope < -5e-5                  # -0.005%/bar → downward
        ctx_macro_note = f"1m_slope={norm_slope * 1e4:.1f}bp/bar"
    elif df5m is not None and len(df5m) >= 8:
        # Fallback: 5m EMA only when we have enough bars to be meaningful
        e5  = float(df5m["close"].ewm(span=5,  adjust=False).mean().iloc[-1])
        e21 = float(df5m["close"].ewm(span=21, adjust=False).mean().iloc[-1])
        ctx_macro_up = e5 > e21
        ctx_macro_dn = e5 < e21
        ctx_macro_note = f"5m_ema5vs21 (n={len(df5m)})"

    # ctx_up/ctx_down — weak context flag passed to strategies (informational)
    ctx_up   = ctx_up_1m  or ctx_macro_up  or mode_obj.trend_up
    ctx_down = ctx_dn_1m  or ctx_macro_dn  or mode_obj.trend_down

    debug_strategies: dict = {}

    def _tier_of(name: str) -> str:
        if name in routing.get("priority", []):
            return "priority"
        if name in routing.get("primary", []):
            return "primary"
        return "secondary"

    def _run_batch(names: list[str]) -> list:
        """Run a list of strategy names, return fired candidates."""
        fired = []
        for name in names:
            fn = _STRATEGY_FNS.get(name)
            if fn is None:
                continue
            if not is_strategy_enabled(name):
                debug_strategies[name] = {"status": "DISABLED", "skipped": True}
                continue
            # Skip strategies disabled for this pair (compatibility matrix)
            if name in _disabled_strats:
                debug_strategies[name] = {"status": "PAIR_DISABLED", "skipped": True}
                continue
            try:
                kwargs = dict(df=df1m, ind=ind, levels=levels,
                              ctx_trend_up=ctx_up, ctx_trend_down=ctx_down)
                # Pass mode to strategies that accept it
                if name in ("level_breakout", "ema_bounce", "rsi_bb_scalp",
                            "three_candle_reversal", "stoch_snap",
                            "ema_micro_cross", "otc_trend_confirm", "double_bottom_top"):
                    kwargs["mode"] = mode_obj.mode
                # Pass pair_params to new strategies for parameter adaptation
                if name in ("rsi_bb_scalp", "three_candle_reversal", "stoch_snap",
                            "ema_micro_cross", "otc_trend_confirm", "double_bottom_top"):
                    kwargs["pair_params"] = _pair_params
                if name == "level_breakout":
                    kwargs["df1m_ctx"] = df1m_ctx
                if name == "level_touch":
                    kwargs["df5m"] = df5m
                res = fn(**kwargs)
            except Exception as e:
                logger.warning("Strategy %s failed: %s", name, e)
                continue

            pct = res.conditions_met / res.total_conditions if res.total_conditions > 0 else 0
            # Pull per-condition breakdown from strategy debug
            # Direction that fired determines which side's conditions to show
            if res.direction == "BUY":
                conds = res.debug.get("buy_conditions", {})
            elif res.direction == "SELL":
                conds = res.debug.get("sell_conditions", {})
            else:
                # NO_SIGNAL: show whichever side had more conditions met
                buy_c  = res.debug.get("buy_conditions", {})
                sell_c = res.debug.get("sell_conditions", {})
                buy_n  = res.debug.get("buy_met", 0)
                sell_n = res.debug.get("sell_met", 0)
                conds  = buy_c if buy_n >= sell_n else sell_c
            debug_strategies[name] = {
                "direction": res.direction,
                "confidence": round(res.confidence, 1),
                "conditions_met": res.conditions_met,
                "total": res.total_conditions,
                "pct": round(pct * 100),
                "tier": _tier_of(name),
                "early_reject": res.debug.get("early_reject"),
                "conditions": conds,
            }
            if res.direction in ("BUY", "SELL") and pct >= 0.37 and res.confidence > 10:
                fired.append(res)
        return fired

    # PRIORITY first (level_touch) — if it fires, skip primary/secondary entirely
    candidates = _run_batch(routing.get("priority", []))
    used_tier = "priority"
    if not candidates:
        # PRIMARY strategies
        candidates = _run_batch(routing["primary"])
        used_tier = "primary"
        if not candidates:
            # SECONDARY strategies (fallback)
            candidates = _run_batch(routing["secondary"])
            used_tier = "secondary"

    # ── Record condition frequencies (fire-and-forget, never blocks signal) ──
    try:
        from db.database import record_condition_evals as _rec_cond
        _evals = [
            (sname, {k: v for k, v in sd.get("conditions", {}).items() if isinstance(v, bool)})
            for sname, sd in debug_strategies.items()
            if not sd.get("skipped")
        ]
        if _evals:
            asyncio.create_task(_rec_cond(_evals))
    except Exception:
        pass

    if not candidates:
        return _no_signal(
            f"Ни одна стратегия не выполнила условия (режим={mode_obj.mode})",
            {
                "mode": mode_obj.mode, "mode_strength": round(mode_obj.strength, 1),
                "mode_debug": mode_obj.debug, "mode_explanation": mode_obj.explanation,
                "strategies": debug_strategies,
                "used_tier": used_tier,
                "ctx_up_1m": ctx_up_1m, "ctx_dn_1m": ctx_dn_1m,
                "ctx_macro_up": ctx_macro_up, "ctx_macro_dn": ctx_macro_dn,
                "ctx_macro_note": ctx_macro_note,
                **_bar_debug, **_ind_dbg, **_lvl_dbg,
            }
        )

    # ── Pick best candidate ────────────────────────────────────────────────────
    best = max(candidates, key=lambda r: r.confidence)

    # If two strategies disagree on direction → pick by confidence, then by priority
    # Lower number = higher priority (wins tie-break)
    _STRATEGY_PRIORITY = {
        # New strategies (primary)
        "rsi_bb_scalp":          1,
        "stoch_snap":            1,
        "three_candle_reversal": 2,
        "ema_micro_cross":       2,
        "otc_trend_confirm":     3,
        "double_bottom_top":     3,
        # Legacy strategies (secondary fallback)
        "level_breakout":        4,
        "ema_bounce":            5,
        "level_touch":           6,
    }

    buy_cands  = [r for r in candidates if r.direction == "BUY"]
    sell_cands = [r for r in candidates if r.direction == "SELL"]
    if buy_cands and sell_cands:
        best_buy  = max(buy_cands,  key=lambda r: r.confidence)
        best_sell = max(sell_cands, key=lambda r: r.confidence)
        if abs(best_buy.confidence - best_sell.confidence) < 6:
            # Confidence too close — break tie by strategy priority
            pri_buy  = _STRATEGY_PRIORITY.get(best_buy.strategy_name,  99)
            pri_sell = _STRATEGY_PRIORITY.get(best_sell.strategy_name, 99)
            if pri_buy == pri_sell:
                # Same strategy on both sides (shouldn't happen) — skip
                return _no_signal(
                    f"Противоречие стратегий — равный conf и приоритет ({best_buy.strategy_name})",
                    {
                        "mode": mode_obj.mode, "mode_strength": round(mode_obj.strength, 1),
                        "mode_debug": mode_obj.debug,
                        "strategies": debug_strategies,
                        "buy_conf": round(best_buy.confidence, 1),
                        "sell_conf": round(best_sell.confidence, 1),
                        **_bar_debug, **_ind_dbg, **_lvl_dbg,
                    }
                )
            # Lower priority number = higher priority strategy wins
            best = best_buy if pri_buy < pri_sell else best_sell
        else:
            best = best_buy if best_buy.confidence > best_sell.confidence else best_sell

    direction = best.direction
    conf_raw  = best.confidence

    # ── Layer 4: Multipliers ───────────────────────────────────────────────────

    # 4a. Multi-timeframe context confirmation / penalty
    # Bonus (+7):  1m EMA direction AND macro slope BOTH agree with signal
    # Penalty ×0.82: BOTH oppose signal (true counter-trend, not just noise)
    ctx_up_strong = ctx_up_1m and ctx_macro_up
    ctx_dn_strong = ctx_dn_1m and ctx_macro_dn

    if direction == "BUY":
        if ctx_up_strong and best.conditions_met >= 5:
            conf_raw += 3        # 1m EMA + slope confirm upward (only solid signals)
        elif ctx_dn_strong:
            conf_raw *= 0.82     # both layers oppose → counter-trend penalty
    else:  # SELL
        if ctx_dn_strong and best.conditions_met >= 5:
            conf_raw += 3
        elif ctx_up_strong:
            conf_raw *= 0.82

    # 4b. Market mode strength multiplier
    mode_str_m = mode_obj.strength / 100.0   # 0-1
    # Only apply if confidence is moderate (don't destroy already-great signals)
    if conf_raw < 75:
        conf_raw = conf_raw * (0.88 + 0.12 * mode_str_m)

    # 4c. Hard floor: if after all multipliers conf < 60% of raw → apply ×0.60
    floor_conf = best.confidence * 0.60
    if conf_raw < floor_conf:
        conf_raw = floor_conf

    conf_raw = min(100.0, conf_raw)

    # 4d. Trend guard: penalise signals strongly against confirmed trend
    # Strong trend = market mode is TRENDING + EMA stack is fully aligned
    strong_down = (mode_obj.mode == "TRENDING_DOWN" and ind.ema5 < ind.ema13 < ind.ema21)
    strong_up   = (mode_obj.mode == "TRENDING_UP"   and ind.ema5 > ind.ema13 > ind.ema21)

    if direction == "BUY" and strong_down:
        conf_raw *= 0.50   # BUY against downtrend: conf halved → likely below threshold
    if direction == "SELL" and strong_up:
        conf_raw *= 0.50   # SELL against uptrend: conf halved → likely below threshold

    # ── Pair confidence adjustment (volatile penalty / calm bonus) ────────────
    if _pair_params and _pair_params.confidence_adj != 0.0:
        conf_raw += _pair_params.confidence_adj
        conf_raw = max(0.0, min(100.0, conf_raw))

    # ── Threshold check ────────────────────────────────────────────────────────
    # Base floors (engine level — keeps /signal working for manual analysis):
    #   normal:  57  (small raise from original 55)
    #   2m:      62  (raised from 55: 53.8% WR at lower threshold)
    #   raised (after 2 losses): 70
    #
    # NOTE: Stricter filtering (62+ global, stoch_snap 70+) is enforced by
    # SignalFilter for auto-signals. Manual /signal uses the engine floor only.
    if raised_threshold:
        min_threshold = 70
    elif best.strategy_name in _TWO_MIN_STRATEGIES:
        min_threshold = 62   # 2m: modest raise; 53.8% WR at old floor
    else:
        min_threshold = 57   # 1m: small raise from 55

    # ── NEUTRAL context penalty: +5 when no directional confirmation ──────────
    # Data: NEUTRAL session = 46.6% WR vs 52% in BULL/BEAR session.
    # If the signal has NO support from either 1m EMA or macro slope,
    # require 5 extra confidence points before letting it through.
    ctx_supports_signal = (
        (direction == "BUY"  and (ctx_up_1m or ctx_macro_up)) or
        (direction == "SELL" and (ctx_dn_1m or ctx_macro_dn))
    )
    if not ctx_supports_signal:
        min_threshold += 5   # need 62/67 instead of 57/62 in true neutral conditions

    if conf_raw < min_threshold:
        return _no_signal(
            f"Уверенность {conf_raw:.0f} < порог {min_threshold} (tier={used_tier})",
            {
                "mode": mode_obj.mode, "mode_strength": round(mode_obj.strength, 1),
                "mode_debug": mode_obj.debug,
                "conf_raw": round(conf_raw, 1),
                "strategy": best.strategy_name,
                "strategies": debug_strategies,
                "used_tier": used_tier,
                "min_threshold": min_threshold,
                "ctx_up_1m": ctx_up_1m, "ctx_dn_1m": ctx_dn_1m,
                "ctx_macro_note": ctx_macro_note,
                **_bar_debug, **_ind_dbg, **_lvl_dbg,
            }
        )

    # ── Stars ──────────────────────────────────────────────────────────────────
    # Tightened bins: data showed 75+ had same 48% WR as 55-60 → bins were too loose
    if   conf_raw >= 80: stars = 5
    elif conf_raw >= 70: stars = 4
    else:                stars = 3

    if   conf_raw >= 80: quality = "strong"
    elif conf_raw >= 70: quality = "good"
    else:                quality = "moderate"

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
            **_bar_debug,
            "mode": mode_obj.mode,
            "mode_strength": round(mode_obj.strength, 1),
            "mode_debug": mode_obj.debug,
            "ctx_up": ctx_up,
            "ctx_down": ctx_down,
            "ctx_up_1m": ctx_up_1m,   "ctx_dn_1m": ctx_dn_1m,
            "ctx_macro_up": ctx_macro_up, "ctx_macro_dn": ctx_macro_dn,
            "ctx_macro_note": ctx_macro_note,
            "ctx_up_strong": ctx_up_strong, "ctx_dn_strong": ctx_dn_strong,
            "used_tier": used_tier,
            "min_threshold": min_threshold,
            "strategies": debug_strategies,
            "best_strategy": best.strategy_name,
            "conf_before_multipliers": round(best.confidence, 1),
            "conf_after_multipliers": round(conf_raw, 1),
            "raised_threshold": raised_threshold,
            "avg_body_pct": round(avg_body_pct, 6),
            "ema_spread_pct": round(ema_spread_pct, 6),
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
    """1m strategies → 1m, dedicated 2m strategies → 2m."""
    if strategy in _TWO_MIN_STRATEGIES:
        return "2m"
    return "1m"


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
