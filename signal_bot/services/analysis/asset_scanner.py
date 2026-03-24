"""
Asset Scanner — evaluates pair TRADABILITY for signal-finding conditions.

Does NOT run full strategy analysis (that happens after user picks a pair).
Evaluates whether market CONDITIONS are favorable for the bot's 6 strategies.

Flow:
  1. User presses "Recommended Pairs"
  2. Scanner checks ALL cached pairs (pure computation, no network)
  3. Returns ranked list of tradable pairs with scores 0-100
  4. User selects pair → full strategy analysis runs on fresh WS data
"""
import logging
import time
from dataclasses import dataclass, field

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import pandas as pd

from .indicators  import Indicators, calculate_indicators
from .levels      import LevelSet, detect_levels, _swing_highs, _swing_lows
from .market_mode import MarketMode, detect_market_mode

logger = logging.getLogger(__name__)

THRESHOLD_HIGH = 65
THRESHOLD_LOW  = 55
MAX_PAIRS      = 7
SCAN_CACHE_TTL = 90   # seconds — reuse results if pressed again quickly


@dataclass
class TradabilityResult:
    symbol: str
    pair:   str
    score:  int
    mode:   str
    payout: int                          = 0
    applicable_strategies: list[str]     = field(default_factory=list)
    levels: dict                         = field(default_factory=dict)
    explanation: str                     = ""
    criteria: dict                       = field(default_factory=dict)


# ── Scan result cache ─────────────────────────────────────────────────────────

_scan_cache: list[TradabilityResult] | None = None
_scan_cache_ts: float = 0.0


def get_scan_cache() -> tuple[list[TradabilityResult] | None, float]:
    """Return (cached_results, age_seconds) or (None, 0)."""
    if _scan_cache is None:
        return None, 0.0
    age = time.time() - _scan_cache_ts
    if age > SCAN_CACHE_TTL:
        return None, 0.0
    return _scan_cache, age


def _store_scan_cache(results: list[TradabilityResult]) -> None:
    global _scan_cache, _scan_cache_ts
    _scan_cache = results
    _scan_cache_ts = time.time()


# ── DataFrame helper ──────────────────────────────────────────────────────────

def _to_df(candles: list[dict]) -> pd.DataFrame | None:
    if not candles or len(candles) < 30:
        return None
    df = pd.DataFrame(candles)
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    if len(df) < 30:
        return None
    return df.reset_index(drop=True)


# ── Criterion 1: Volatility Fitness (25%) ────────────────────────────────────

def calculate_volatility_score(ind) -> int:
    """Uses atr_ratio from Indicators dataclass."""
    try:
        ratio = ind.atr_ratio
        if ratio < 0.3:   return 5
        if ratio < 0.5:   return 20
        if ratio < 0.7:   return 50
        if ratio < 0.9:   return 80
        if ratio < 1.3:   return 100
        if ratio < 1.8:   return 80
        if ratio < 2.5:   return 45
        if ratio < 3.5:   return 20
        return 5
    except Exception:
        return 50


# ── Criterion 2: Structure Clarity (20%) ─────────────────────────────────────

def calculate_structure_score(df: pd.DataFrame, ind) -> int:
    """Direction changes + body ratio + EMA alignment."""
    try:
        last15 = df.tail(15)
        closes = last15["close"].values
        opens  = last15["open"].values

        directions = [1 if closes[i] > opens[i] else -1 for i in range(len(closes))]
        direction_changes = sum(
            1 for i in range(1, len(directions)) if directions[i] != directions[i - 1]
        )

        bodies     = abs(last15["close"] - last15["open"])
        ranges     = last15["high"] - last15["low"]
        body_ratio = float((bodies / ranges.replace(0, float("nan"))).mean())
        if pd.isna(body_ratio):
            body_ratio = 0.3

        ema_aligned = (ind.ema5 > ind.ema13 > ind.ema21) or (ind.ema5 < ind.ema13 < ind.ema21)

        if   direction_changes <= 4 and body_ratio >= 0.50: score = 95
        elif direction_changes <= 5 and body_ratio >= 0.45: score = 85
        elif direction_changes <= 6 and body_ratio >= 0.40: score = 70
        elif direction_changes <= 8 and body_ratio >= 0.35: score = 55
        elif direction_changes <= 9 and body_ratio >= 0.30: score = 40
        else:                                                score = 15

        if ema_aligned:
            score = min(100, score + 10)
        return score
    except Exception:
        return 50


# ── Criterion 3: Level Quality (15%) ─────────────────────────────────────────

def calculate_level_score(levels) -> tuple[int, dict]:
    """Uses LevelSet from detect_levels()."""
    try:
        current_price_ref = None  # not needed for scoring

        sups = levels.supports
        ress = levels.resistances
        strong_sups = levels.strong_sup
        strong_ress = levels.strong_res

        # Count significant levels (2+ touches are already filtered in LevelSet)
        n_sig = len(sups) + len(ress)
        n_strong = len(strong_sups) + len(strong_ress)

        # Nearest distance between closest sup and res
        nearest_dist = 0.0
        if sups and ress:
            nearest_s = sups[0]  # nearest_sup is the closest one
            nearest_r = ress[0]  # nearest_res
            if nearest_s > 0:
                nearest_dist = (nearest_r - nearest_s) / nearest_s * 100

        levels_info = {
            "supports":    [{"price": p, "touches": 2} for p in sups],
            "resistances": [{"price": p, "touches": 2} for p in ress],
            "strong_sup":  strong_sups,
            "strong_res":  strong_ress,
        }
        # Mark strong levels with higher touch count
        for lv in levels_info["supports"]:
            if lv["price"] in strong_sups:
                lv["touches"] = 3
        for lv in levels_info["resistances"]:
            if lv["price"] in strong_ress:
                lv["touches"] = 3

        if   n_sig >= 3 and nearest_dist > 0.20: score = 95
        elif n_sig >= 3:                          score = 80
        elif n_sig == 2 and nearest_dist > 0.15: score = 70
        elif n_sig == 2:                          score = 55
        elif n_sig == 1:                          score = 35
        else:                                     score = 10

        return score, levels_info
    except Exception:
        return 10, {}


# ── Criterion 4: Strategy Availability (25%) ─────────────────────────────────

def calculate_strategy_availability(
    df: pd.DataFrame,
    ind,
    levels,
    levels_info: dict,
) -> tuple[int, list[str]]:
    """Check prerequisites (not full signals) for each of the 6 strategies."""
    applicable = []
    try:
        current = float(df["close"].iloc[-1])
        last10  = df.tail(10)
        last5   = df.tail(5)
        last20  = df.tail(20)
        atr_ratio = ind.atr_ratio

        ema_aligned = (ind.ema5 > ind.ema13 > ind.ema21) or \
                      (ind.ema5 < ind.ema13 < ind.ema21)

        # Candles in trend direction (last 10)
        if ind.ema5 > ind.ema13:
            trend_candles = int((last10["close"] > last10["open"]).sum())
        else:
            trend_candles = int((last10["close"] < last10["open"]).sum())

        sig_levels = levels_info.get("supports", []) + levels_info.get("resistances", [])
        sig_prices = [lv["price"] for lv in sig_levels]

        # ── 1. EMA Bounce ─────────────────────────────────────
        if ema_aligned and atr_ratio > 0.5 and trend_candles >= 3:
            applicable.append("EMA Bounce")

        # ── 2. Squeeze Breakout ───────────────────────────────
        bb_narrowing = ind.bb_bw < ind.bb_bw_prev * 1.05
        avg_body_30  = float(abs(df["close"] - df["open"]).tail(30).mean())
        avg_body_5   = float(abs(last5["close"] - last5["open"]).mean())
        body_squeezed = avg_body_30 > 0 and avg_body_5 < avg_body_30 * 0.6
        if (atr_ratio < 0.7 and body_squeezed and bb_narrowing) or \
           (atr_ratio < 0.7 and bb_narrowing):
            applicable.append("Squeeze Breakout")

        # ── 3. Level Bounce ───────────────────────────────────
        near_level = any(
            abs(p - current) / current < 0.003 for p in sig_prices
        )
        if sig_levels and near_level:
            applicable.append("Level Bounce")

        # ── 4. RSI Reversal ───────────────────────────────────
        rsi_now  = ind.rsi
        rsi_extreme        = rsi_now < 25 or rsi_now > 75
        rsi_recent_extreme = False  # simplified (no series available here)
        last_c = df["close"].tail(5).values
        consec_up = all(last_c[i] < last_c[i + 1] for i in range(len(last_c) - 1))
        consec_dn = all(last_c[i] > last_c[i + 1] for i in range(len(last_c) - 1))
        consec_move = (consec_up or consec_dn) and atr_ratio > 0.7
        if rsi_extreme or rsi_recent_extreme or consec_move:
            applicable.append("RSI Reversal")

        # ── 5. Micro Breakout ─────────────────────────────────
        very_near = any(abs(p - current) / current < 0.002 for p in sig_prices)
        level_tested_recently = False
        for p in sig_prices:
            touches_20 = int(
                ((last20["high"] - p).abs() / p < 0.002).sum() +
                ((last20["low"]  - p).abs() / p < 0.002).sum()
            )
            if touches_20 >= 2:
                level_tested_recently = True
                break
        if sig_levels and very_near and level_tested_recently:
            applicable.append("Micro Breakout")

        # ── 6. Divergence ─────────────────────────────────────
        sh20 = _swing_highs(last20["high"], window=2)
        sl20 = _swing_lows(last20["low"],   window=2)
        # RSI range from ema series — approximate using atr as proxy
        # Use bb_bw as volatility proxy for RSI range estimation
        rsi_range_ok = ind.bb_bw > 0.001  # proxy: meaningful BB width implies RSI moved
        if (len(sh20) >= 2 or len(sl20) >= 2) and rsi_range_ok and atr_ratio > 0.5:
            applicable.append("Divergence")

    except Exception as exc:
        logger.debug("Strategy availability error: %s", exc)

    n = len(applicable)
    if   n >= 4: score = 100
    elif n == 3: score = 80
    elif n == 2: score = 60
    elif n == 1: score = 35
    else:        score = 5

    return score, applicable


# ── Criterion 5: Candle Size (10%) ───────────────────────────────────────────

def calculate_candle_size_score(df: pd.DataFrame) -> int:
    try:
        last20  = df.tail(20)
        avg_body = float(abs(last20["close"] - last20["open"]).mean())
        current  = float(df["close"].iloc[-1])
        if current == 0:
            return 50
        pct = avg_body / current * 100
        if   pct < 0.003: return 5
        if   pct < 0.006: return 25
        if   pct < 0.012: return 55
        if   pct < 0.030: return 85
        if   pct < 0.060: return 100
        if   pct < 0.100: return 75
        return 30
    except Exception:
        return 50


# ── Criterion 6: Anomaly Absence (5%) ────────────────────────────────────────

def calculate_anomaly_score(df: pd.DataFrame) -> int:
    try:
        last10  = df.tail(10)
        bodies  = abs(last10["close"] - last10["open"])
        avg_b10 = float(bodies.mean())
        spikes  = int((bodies > 4 * avg_b10).sum()) if avg_b10 > 0 else 0

        closes = last10["close"].values
        gaps   = sum(
            1 for i in range(1, len(closes))
            if closes[i - 1] > 0 and abs(closes[i] - closes[i - 1]) / closes[i - 1] > 0.0003
        )

        ranges   = last10["high"] - last10["low"]
        doji_pct = float(
            (bodies < 0.15 * ranges.replace(0, float("nan"))).sum() / len(last10)
        )

        if   spikes == 0 and gaps == 0 and doji_pct < 0.3: return 95
        elif spikes <= 1 and gaps == 0:                     return 75
        elif spikes <= 1 and gaps <= 1:                     return 55
        elif spikes >= 2 or doji_pct >= 0.5:                return 25
        else:                                               return 5
    except Exception:
        return 75


# ── Market mode label (Russian) ───────────────────────────────────────────────

def _mode_label_ru(mode: str) -> str:
    return {
        "TRENDING_UP":   "📈 Тренд вверх",
        "TRENDING_DOWN": "📉 Тренд вниз",
        "RANGE":         "↔️ Боковик",
        "VOLATILE":      "⚡ Волатильный",
        "SQUEEZE":       "🔲 Сжатие",
    }.get(mode, mode)


# ── Specific explanation builder ──────────────────────────────────────────────

def _build_explanation(df: pd.DataFrame, ind, levels_info: dict, applicable: list[str], mode: str) -> str:
    try:
        current = float(df["close"].iloc[-1])
        parts   = []

        if "EMA Bounce" in applicable:
            direction = "вверх" if ind.ema5 > ind.ema13 else "вниз"
            parts.append(f"EMA выровнены {direction} ({ind.ema5:.5g}/{ind.ema13:.5g}).")

        sups = levels_info.get("supports", [])
        ress = levels_info.get("resistances", [])
        all_lvls = sups + ress
        if all_lvls and ("Level Bounce" in applicable or "Micro Breakout" in applicable):
            nearest = min(all_lvls, key=lambda lv: abs(lv["price"] - current))
            dist    = abs(nearest["price"] - current) / current * 100
            parts.append(f"Уровень {nearest['price']:.5g} ({nearest['touches']} касания, {dist:.2f}% от цены).")

        if "RSI Reversal" in applicable:
            parts.append(f"RSI {ind.rsi:.0f} — условия разворота.")

        if "Squeeze Breakout" in applicable:
            parts.append("Волатильность сжата, BB сужаются — ожидаем пробой.")

        if "Divergence" in applicable and not parts:
            parts.append("Дивергенция RSI на свинговых экстремумах.")

        n = len(applicable)
        if n >= 3:
            parts.append(f"{n} стратегии доступны.")

        if not parts:
            parts.append(f"Режим: {_mode_label_ru(mode)}. Условия подходящие.")

        return " ".join(parts[:3])
    except Exception:
        return f"Режим: {_mode_label_ru(mode)}. {len(applicable)} стратегий доступны."


# ── Main tradability calculator ───────────────────────────────────────────────

def calculate_tradability(symbol: str, pair: str, candles: list[dict]) -> TradabilityResult | None:
    """Full tradability calculation for one pair. Pure computation."""
    df = _to_df(candles)
    if df is None:
        return None

    try:
        ind    = calculate_indicators(df)
        levels = detect_levels(df)
        mm     = detect_market_mode(df)

        # Build levels_info dict for downstream use
        lvl_score, levels_info = calculate_level_score(levels)

        vol_score  = calculate_volatility_score(ind)
        str_score  = calculate_structure_score(df, ind)
        avl_score, applicable = calculate_strategy_availability(df, ind, levels, levels_info)
        csz_score  = calculate_candle_size_score(df)
        anom_score = calculate_anomaly_score(df)

        score = int(
            vol_score  * 0.25 +
            str_score  * 0.20 +
            lvl_score  * 0.15 +
            avl_score  * 0.25 +
            csz_score  * 0.10 +
            anom_score * 0.05
        )

        explanation = _build_explanation(df, ind, levels_info, applicable, mm.mode)

        return TradabilityResult(
            symbol               = symbol,
            pair                 = pair,
            score                = score,
            mode                 = mm.mode,
            applicable_strategies= applicable,
            levels               = levels_info,
            explanation          = explanation,
            criteria             = {
                "volatility":           vol_score,
                "structure":            str_score,
                "levels":               lvl_score,
                "strategy_availability":avl_score,
                "candle_size":          csz_score,
                "anomaly":              anom_score,
                "atr_ratio":            round(ind.atr_ratio, 3),
            },
        )
    except Exception as exc:
        logger.warning("Tradability calc failed for %s: %s", symbol, exc)
        return None


# ── scan_pairs_fresh ──────────────────────────────────────────────────────────

async def scan_pairs_fresh(
    pairs_map: dict[str, str],
    payout_map: dict[str, int] | None = None,
) -> list[TradabilityResult]:
    """
    Real-time scan: refresh all pairs via WS, run full strategy engine,
    return pairs where at least one strategy met >= 3 conditions (good market).
    Sorted by quality score (higher = better), max MAX_PAIRS.
    """
    import asyncio
    from services.candle_cache import _refresh_all_via_ws, get_cached
    from services.strategy_engine import calculate_signal

    payout_map = payout_map or {}
    symbols = list(pairs_map.keys())

    # 1. Force fresh WS fetch for all pairs
    await _refresh_all_via_ws(symbols)

    results: list[TradabilityResult] = []

    for symbol in symbols:
        candles = get_cached(symbol)
        if not candles or len(candles) < 30:
            continue
        try:
            result = await calculate_signal(candles)
        except Exception as exc:
            logger.warning("Engine failed for %s: %s", symbol, exc)
            continue

        # result.details contains the full debug breakdown
        details: dict = result.details if isinstance(result.details, dict) else {}
        debug: dict = details.get("debug", {})
        direction: str = result.direction  # "BUY" | "SELL" | "NO_SIGNAL"

        is_v2 = "pattern_first" in debug.get("engine", "")

        if is_v2:
            # V2 pattern-first: no "strategies" dict / "conditions_met".
            # Use pattern candidates as the market-health proxy.
            # candidates_checked → no_patterns_passed; all_patterns → score_below_threshold
            n_candidates = len(debug.get("candidates_checked", [])) or len(debug.get("all_patterns", []))
            if direction not in ("BUY", "SELL") and n_candidates == 0:
                # Truly dead market — no patterns detected at all
                continue

            if direction in ("BUY", "SELL"):
                # Real signal: use raw confidence score
                quality_score = int(details.get("confidence_raw", 70))
            else:
                # Patterns were found but didn't pass threshold/filters —
                # pair has structure, worth showing as a candidate
                quality_score = max(30, n_candidates * 20)

        else:
            # V1 legacy path: conditions_met across strategy dict
            strategies: dict = debug.get("strategies", {})
            max_met = 0
            for sd in strategies.values():
                if sd.get("skipped") or sd.get("early_reject"):
                    continue
                met = sd.get("conditions_met", 0)
                if met > max_met:
                    max_met = met

            if max_met < 3:
                continue

            if direction in ("BUY", "SELL"):
                quality_score = int(details.get("confidence_raw", max_met * 10))
            else:
                quality_score = int(debug.get("conf_after_multipliers", max_met * 10))

        market_mode = details.get("market_mode") or debug.get("mode", "")
        label = pairs_map.get(symbol, symbol)
        results.append(TradabilityResult(
            symbol=symbol,
            pair=label,
            score=quality_score,
            mode=market_mode,
            payout=payout_map.get(symbol, 0),
        ))

    results.sort(key=lambda r: r.score, reverse=True)
    return results[:MAX_PAIRS]


# ── scan_all_pairs ────────────────────────────────────────────────────────────

def scan_all_pairs(
    pairs_map: dict[str, str],
    payout_map: dict[str, int] | None = None,
) -> list[TradabilityResult]:
    """
    Scan all cached pairs using tradability criteria.
    Uses ONLY cached candles — no network calls.
    pairs_map: {symbol: label}
    payout_map: {symbol: payout_pct}  (optional)
    Returns filtered+sorted list (score >= threshold, max 7).
    """
    from services.candle_cache import get_cached

    payout_map = payout_map or {}
    results: list[TradabilityResult] = []
    for symbol, pair in pairs_map.items():
        candles = get_cached(symbol)
        if not candles or len(candles) < 30:
            continue
        result = calculate_tradability(symbol, pair, candles)
        if result is not None:
            result.payout = payout_map.get(symbol, 0)
            results.append(result)

    threshold = THRESHOLD_HIGH
    filtered  = [r for r in results if r.score >= threshold]
    if not filtered:
        threshold = THRESHOLD_LOW
        filtered  = [r for r in results if r.score >= threshold]

    filtered.sort(key=lambda r: r.score, reverse=True)
    filtered = filtered[:MAX_PAIRS]

    _store_scan_cache(filtered)
    return filtered


# ── format_scan_output ────────────────────────────────────────────────────────

def format_scan_output(results: list[TradabilityResult], scan_age_sec: float = 0) -> str:
    """Format tradability scan results for Telegram HTML."""
    lines = [
        "📋 <b>Пары с благоприятными условиями</b>",
        "",
    ]

    for i, r in enumerate(results, 1):
        payout_str = f"  •  {r.payout}%" if r.payout else ""
        lines.append(f"{i}. <b>{r.pair}</b>{payout_str}")

    lines.append("")
    lines.append("Выберите пару для анализа")
    return "\n".join(lines)
