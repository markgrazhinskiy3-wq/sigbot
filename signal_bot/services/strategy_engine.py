"""
Strategy Engine — entry point for signal calculation.
Delegates to the new price-action scoring engine.
"""
import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services.analysis.scoring_engine import run_scoring_engine

logger = logging.getLogger(__name__)


@dataclass
class SignalResult:
    direction:    str   # "BUY" | "SELL" | "NO_SIGNAL"
    confidence:   int   # 0-5 for UI display
    details:      dict  # full analysis breakdown


def calculate_signal(candles: list[dict]) -> SignalResult:
    """
    Receives a list of OHLC dicts:
        [{"open": float, "high": float, "low": float, "close": float}, ...]
    Returns a SignalResult.
    """
    if len(candles) < 20:
        logger.warning("Not enough candles (%d < 20)", len(candles))
        return SignalResult("NO_SIGNAL", 0, {"error": "not enough data"})

    df = pd.DataFrame(candles)
    df = df.rename(columns=str.lower)
    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])

    if len(df) < 20:
        return SignalResult("NO_SIGNAL", 0, {"error": "bad candle data"})

    result = run_scoring_engine(df)

    return SignalResult(
        direction=result["direction"],
        confidence=result["confidence_5"],
        details=result,
    )
