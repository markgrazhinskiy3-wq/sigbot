"""
Candle Validator & Preprocessor
Ensures OHLC data is clean, correctly ordered, and returns diagnostics.
"""
import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    ok: bool
    candles_count: int
    candles_after_clean: int
    order: str                   # "old_to_new" | "new_to_old" | "unknown"
    last_close: float
    avg_body_pct: float          # average body size as % of price
    issues: list = field(default_factory=list)


def validate_and_fix(raw: list[dict]) -> tuple[pd.DataFrame | None, ValidationResult]:
    """
    Accepts raw candle dicts: [{open, high, low, close}, ...]
    Returns (cleaned_df, ValidationResult).
    df is None if data is unusable.
    """
    n_raw = len(raw)

    if n_raw < 5:
        return None, ValidationResult(
            ok=False, candles_count=n_raw, candles_after_clean=0,
            order="unknown", last_close=0.0, avg_body_pct=0.0,
            issues=["Слишком мало свечей"]
        )

    # ── Step 1: normalise keys ────────────────────────────────────────────────
    records = []
    for c in raw:
        try:
            o = float(c.get("open")  or c.get("o") or 0)
            h = float(c.get("high")  or c.get("h") or 0)
            l = float(c.get("low")   or c.get("l") or 0)
            cl = float(c.get("close") or c.get("c") or 0)
            if o > 0 and h > 0 and l > 0 and cl > 0:
                records.append({"open": o, "high": h, "low": l, "close": cl})
        except (TypeError, ValueError):
            continue

    issues = []
    if len(records) < n_raw:
        issues.append(f"Отброшено {n_raw - len(records)} битых свечей")

    if len(records) < 5:
        return None, ValidationResult(
            ok=False, candles_count=n_raw, candles_after_clean=len(records),
            order="unknown", last_close=0.0, avg_body_pct=0.0,
            issues=issues + ["После очистки осталось < 5 свечей"]
        )

    # ── Step 2: detect and fix candle order ───────────────────────────────────
    # Compare first vs last close — if first > last for downtrend or vice versa,
    # just detect by checking if data is monotonically increasing on timestamps.
    # Since we have no timestamps, we use a heuristic: check if the last candle
    # looks "more recent" (for OTC, newer data usually has smaller absolute changes).
    # Simple approach: try both orders, pick the one where OHLC constraints hold better.
    order = _detect_order(records)
    if order == "new_to_old":
        records = list(reversed(records))
        issues.append("Свечи были в обратном порядке — перевёрнуты")
        order = "old_to_new"

    # ── Step 3: fix OHLC constraint violations ────────────────────────────────
    fixed = []
    for r in records:
        o, h, l, c = r["open"], r["high"], r["low"], r["close"]
        actual_h = max(o, h, l, c)
        actual_l = min(o, h, l, c)
        if h != actual_h or l != actual_l:
            issues.append("Исправлены OHLC нарушения (high/low)")
        fixed.append({"open": o, "high": actual_h, "low": actual_l, "close": c})

    df = pd.DataFrame(fixed)
    last_close = float(df["close"].iloc[-1])

    body_abs = (df["close"] - df["open"]).abs()
    avg_body_pct = float(body_abs.mean() / last_close * 100) if last_close > 0 else 0.0

    logger.info(
        "Candles: %d raw → %d clean | order=%s | last=%.5f | avg_body=%.4f%%",
        n_raw, len(fixed), order, last_close, avg_body_pct
    )

    return df, ValidationResult(
        ok=True,
        candles_count=n_raw,
        candles_after_clean=len(fixed),
        order=order,
        last_close=last_close,
        avg_body_pct=avg_body_pct,
        issues=issues,
    )


def _detect_order(records: list[dict]) -> str:
    """
    Heuristic: count how many consecutive pairs satisfy close[i] close to open[i+1]
    (gapless = correct order).  If reversed version fits better → new_to_old.
    """
    def gap_score(recs):
        gaps = [abs(recs[i]["close"] - recs[i+1]["open"]) for i in range(len(recs)-1)]
        if not gaps:
            return 0.0
        avg_gap = sum(gaps) / len(gaps)
        avg_body = sum(abs(r["close"] - r["open"]) for r in recs) / len(recs) or 1e-8
        return avg_gap / avg_body  # lower = better order

    rev = list(reversed(records))
    score_fwd = gap_score(records)
    score_rev = gap_score(rev)

    if score_rev < score_fwd * 0.7:
        return "new_to_old"
    return "old_to_new"
