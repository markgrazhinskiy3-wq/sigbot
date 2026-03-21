"""
Indicator Confirmation (secondary role only)
EMA / RSI / Stochastic / Momentum — used to confirm, not to originate signals.
"""
import numpy as np
import pandas as pd
from dataclasses import dataclass


@dataclass
class IndicatorConfirmationResult:
    ema_signal:  str   # buy | sell | neutral
    rsi_signal:  str   # buy | sell | neutral
    stoch_signal: str  # buy | sell | neutral
    mom_signal:  str   # buy | sell | neutral
    buy_score:   float # 0-100
    sell_score:  float # 0-100
    explanation: str


def indicator_confirmation(df: pd.DataFrame) -> IndicatorConfirmationResult:
    n = len(df)
    if n < 21:
        return IndicatorConfirmationResult(
            "neutral", "neutral", "neutral", "neutral",
            50.0, 50.0, "Недостаточно данных для индикаторов"
        )

    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    price = float(close.iloc[-1])

    # ── EMA cross (9 / 21) ───────────────────────────────────────────────────
    ema9  = float(close.ewm(span=9,  adjust=False).mean().iloc[-1])
    ema21 = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
    ema_spread = abs(ema9 - ema21) / price if price else 0
    if ema_spread < 0.0001:
        ema_signal = "neutral"
    else:
        ema_signal = "buy" if ema9 > ema21 else "sell"

    # ── RSI(14) ───────────────────────────────────────────────────────────────
    rsi = _rsi(close, 14)
    if rsi < 38:
        rsi_signal = "buy"
    elif rsi > 62:
        rsi_signal = "sell"
    else:
        rsi_signal = "neutral"

    # ── Stochastic (5, 3) ─────────────────────────────────────────────────────
    stoch_k = _stoch_k(high, low, close, k=5, d=3)
    if stoch_k < 25:
        stoch_signal = "buy"
    elif stoch_k > 75:
        stoch_signal = "sell"
    else:
        stoch_signal = "neutral"

    # ── Momentum (10) ─────────────────────────────────────────────────────────
    if n > 10:
        mom = float(close.iloc[-1]) - float(close.iloc[-11])
        mom_pct = abs(mom) / price if price else 0
        if mom_pct < 0.0002:
            mom_signal = "neutral"
        else:
            mom_signal = "buy" if mom > 0 else "sell"
    else:
        mom_signal = "neutral"

    # ── Aggregate ─────────────────────────────────────────────────────────────
    all_signals = [ema_signal, rsi_signal, stoch_signal, mom_signal]
    buy_votes  = all_signals.count("buy")
    sell_votes = all_signals.count("sell")

    # Each confirming indicator adds ~25 points (4 total = 100)
    buy_score  = float(buy_votes  * 25)
    sell_score = float(sell_votes * 25)

    parts = [
        f"EMA:{ema_signal.upper()}",
        f"RSI:{rsi_signal.upper()}({rsi:.0f})",
        f"Stoch:{stoch_signal.upper()}({stoch_k:.0f})",
        f"Mom:{mom_signal.upper()}",
    ]

    return IndicatorConfirmationResult(
        ema_signal=ema_signal,
        rsi_signal=rsi_signal,
        stoch_signal=stoch_signal,
        mom_signal=mom_signal,
        buy_score=buy_score,
        sell_score=sell_score,
        explanation=" | ".join(parts),
    )


# ── Helpers ──────────────────────────────────────────────────────────────────

def _rsi(close: pd.Series, period: int = 14) -> float:
    try:
        import pandas_ta as ta
        s = ta.rsi(close, length=period)
        v = float(s.iloc[-1]) if s is not None else 50.0
        return v if not np.isnan(v) else 50.0
    except Exception:
        delta = close.diff()
        gain  = delta.clip(lower=0)
        loss  = (-delta).clip(lower=0)
        ag = gain.ewm(com=period - 1, min_periods=period).mean()
        al = loss.ewm(com=period - 1, min_periods=period).mean()
        rs = ag / al.replace(0, np.nan)
        r  = 100 - (100 / (1 + rs))
        v  = float(r.iloc[-1])
        return v if not np.isnan(v) else 50.0


def _stoch_k(high: pd.Series, low: pd.Series, close: pd.Series,
             k: int = 5, d: int = 3) -> float:
    try:
        import pandas_ta as ta
        s = ta.stoch(high, low, close, k=k, d=d, smooth_k=d)
        col = f"STOCHk_{k}_{d}_{d}"
        v = float(s[col].iloc[-1]) if s is not None and col in s.columns else 50.0
        return v if not np.isnan(v) else 50.0
    except Exception:
        ll = low.rolling(k).min()
        hh = high.rolling(k).max()
        denom = (hh - ll).replace(0, np.nan)
        sk = 100 * (close - ll) / denom
        v  = float(sk.iloc[-1])
        return v if not np.isnan(v) else 50.0
