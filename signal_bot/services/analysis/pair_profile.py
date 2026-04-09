"""
Pair Profile — classifies OTC pairs and provides:
  1. Category (A/B/C/D) — tradability tier
  2. PairType (volatile / calm / normal) — parameter regime
  3. Compatible strategies — which of the 6 strategies fire on this pair
  4. PairParams — threshold overrides passed to strategy functions

Classification source: OTC pair suitability document (April 2026).
Category D pairs are not included — they are removed from config.py.
"""
from __future__ import annotations
from dataclasses import dataclass, field


# ── Pair type ─────────────────────────────────────────────────────────────────

class PairType:
    VOLATILE = "volatile"   # GBP/*, EUR/JPY, AUD/JPY, EUR/TRY — wider params
    CALM     = "calm"       # USD/CHF, NZD/USD, USD/CAD, AUD/CAD, AUD/NZD — tighter
    NORMAL   = "normal"     # everything else — standard params


# ── Per-pair type mapping ─────────────────────────────────────────────────────
# symbol → PairType
_PAIR_TYPE: dict[str, str] = {
    # Volatile (Category B-C — larger movements, wider BB/RSI needed)
    "#GBPUSD_otc":  PairType.VOLATILE,
    "#EURJPY_otc":  PairType.VOLATILE,
    "#GBPAUD_otc":  PairType.VOLATILE,
    "#AUDJPY_otc":  PairType.VOLATILE,
    "#CHFJPY_otc":  PairType.VOLATILE,
    "#NZDJPY_otc":  PairType.VOLATILE,
    "#CADJPY_otc":  PairType.VOLATILE,
    "#EURTRY_otc":  PairType.VOLATILE,

    # Calm (Category A-B — slow/narrow movement, tighter params for cleaner signals)
    "#USDCAD_otc":  PairType.CALM,
    "#AUDCAD_otc":  PairType.CALM,
    "#NZDUSD_otc":  PairType.CALM,
    "#AUDNZD_otc":  PairType.CALM,
    "#EURCHF_otc":  PairType.CALM,
    "#AUDCHF_otc":  PairType.CALM,
}


def get_pair_type(symbol: str) -> str:
    return _PAIR_TYPE.get(symbol, PairType.NORMAL)


# ── Strategy compatibility matrix ─────────────────────────────────────────────
# Strategies disabled (❌) per symbol.
# ⚡ (works with adaptation) → strategy is still enabled, but uses adapted params.
# If a pair is not listed → all 6 strategies are enabled.

_DISABLED_STRATEGIES: dict[str, set[str]] = {
    # EUR/JPY: Three Candle Reversal too noisy (wide swings produce false series)
    "#EURJPY_otc": {"three_candle_reversal"},

    # GBP/AUD: Maximum volatility — only MACD Trend is reliable
    "#GBPAUD_otc": {
        "rsi_bb_scalp",
        "three_candle_reversal",
        "stoch_snap",
        "ema_micro_cross",
        "double_bottom_top",
    },

    # AUD/JPY: Only Stoch + MACD work (Category C)
    "#AUDJPY_otc": {
        "rsi_bb_scalp",
        "three_candle_reversal",
        "ema_micro_cross",
        "double_bottom_top",
    },

    # AUD/USD: EMA & MACD focused — RSI+BB and 3candle work with adaptation
    # (no hard disables — all enabled with volatile-ish params)
    "#AUDUSD_otc": set(),
}


def get_disabled_strategies(symbol: str) -> set[str]:
    """Return set of strategy names that should NOT run on this pair."""
    return _DISABLED_STRATEGIES.get(symbol, set())


def is_strategy_allowed_for_pair(strategy_name: str, symbol: str) -> bool:
    return strategy_name not in get_disabled_strategies(symbol)


# ── Parameter overrides per pair type ────────────────────────────────────────

@dataclass
class PairParams:
    """Strategy parameter overrides. Defaults = standard (normal) values."""
    pair_type: str = PairType.NORMAL

    # RSI thresholds
    rsi_oversold:  float = 30.0
    rsi_overbought: float = 70.0

    # Bollinger Bands std deviation
    bb_std: float = 2.0

    # Stochastic
    stoch_os:  float = 20.0    # oversold level
    stoch_ob:  float = 80.0    # overbought level
    stoch_rsi_os: float = 40.0 # RSI confirmation for stoch oversold
    stoch_rsi_ob: float = 60.0 # RSI confirmation for stoch overbought

    # Three Candle Reversal
    allow_4_candles: bool = False  # accept 4-candle series (volatile pairs)

    # MACD/RSI filter strictness (calm pairs use tighter RSI filter)
    macd_rsi_bull: float = 50.0   # RSI must be above this for BUY confirmation
    macd_rsi_bear: float = 50.0   # RSI must be below this for SELL confirmation

    # EMA Micro-Cross: fast/slow spans
    ema_fast_span: int = 3
    ema_slow_span: int = 8

    # Minimum confidence bonus/penalty applied in engine
    # Positive = bonus for reliable pairs, negative = penalty for noisy ones
    confidence_adj: float = 0.0


def get_pair_params(symbol: str) -> PairParams:
    """Return parameter overrides for the given OTC symbol."""
    pt = get_pair_type(symbol)

    if pt == PairType.VOLATILE:
        return PairParams(
            pair_type       = PairType.VOLATILE,
            rsi_oversold    = 25.0,
            rsi_overbought  = 75.0,
            bb_std          = 2.2,
            stoch_os        = 15.0,
            stoch_ob        = 85.0,
            stoch_rsi_os    = 35.0,
            stoch_rsi_ob    = 65.0,
            allow_4_candles = True,
            macd_rsi_bull   = 50.0,
            macd_rsi_bear   = 50.0,
            ema_fast_span   = 5,
            ema_slow_span   = 13,
            confidence_adj  = -3.0,   # slight penalty for noisy pairs
        )

    if pt == PairType.CALM:
        return PairParams(
            pair_type       = PairType.CALM,
            rsi_oversold    = 35.0,
            rsi_overbought  = 65.0,
            bb_std          = 1.8,
            stoch_os        = 25.0,
            stoch_ob        = 75.0,
            stoch_rsi_os    = 45.0,
            stoch_rsi_ob    = 55.0,
            allow_4_candles = False,
            macd_rsi_bull   = 52.0,
            macd_rsi_bear   = 48.0,
            ema_fast_span   = 3,
            ema_slow_span   = 8,
            confidence_adj  = +2.0,   # bonus for cleaner signals
        )

    # NORMAL — standard parameters
    return PairParams(pair_type=PairType.NORMAL)
