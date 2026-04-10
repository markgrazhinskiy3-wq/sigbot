"""
SignalFilter — 15-layer gate that decides whether a signal gets sent to Telegram.

Architecture:
  - Filters 1-8:  quality / pair / strategy checks (stateless per call)
  - Filters 9-15: session / rate-limit / streak checks (stateful in-memory)
  - After all 15: confidence recalculated using historical WR, not raw engine score

Session derivation from signal dict:
  'session' key → "BULL" | "BEAR" | "NEUTRAL"
  (caller derives from details["debug"] ctx flags before calling check())
"""
from __future__ import annotations

import time
import logging
from collections import deque
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Default config — single source of truth for all filter thresholds
# ─────────────────────────────────────────────────────────────────────────────

def _default_config() -> dict:
    return {
        # Pairs we ALWAYS send signals for (WR ≥ 60% consistently)
        "whitelisted_pairs": {
            "EUR/CHF OTC",     # 100% WR (3 trades, new data)
            "USD/CAD OTC",     # whitelist — marginal but historically ok
        },
        # Grey pairs: send only when confidence meets higher bar
        # Updated thresholds from 102-trade CSV analysis (Apr 2026)
        "grey_pairs": {
            "AUD/CHF OTC": 65,  # was whitelist; now 40% WR (5 trades) — require 65
            "AUD/CAD OTC": 68,  # 25% WR → raised from 62, near-block
            "AUD/USD OTC": 65,  # unchanged
        },
        # Pairs we NEVER send (WR < 50% — tested or structurally weak)
        "blacklisted_pairs": {
            "GBP/AUD OTC",    # 23.1% WR (old test, 13 trades)
            "BHD/CNY OTC",    # 22.2% WR
            "KES/USD OTC",    # 35.7% WR (old test — conflicts with new data, need more)
            "CHF/JPY OTC",    # 35.7% WR
            "AUD/JPY OTC",    # 38.5% WR
            "USD/JPY OTC",    # 41.7% WR (old test — 100% in new, but only 3 trades)
            "EUR/JPY OTC",    # 43.8% WR
            "GBP/USD OTC",    # 44.4% WR
            "NZD/JPY OTC",    # 45.5% WR
            "AUD/NZD OTC",    # 40.0% WR (confirmed bad in both tests)
            "NZD/USD OTC",    # 50.0% WR
            "AED/CNY OTC",    # 53.3% WR (marginal)
        },
        # Strategies approved for Telegram delivery
        # Updated: ema_micro_cross added (69% WR, 29 trades — best performer)
        # stoch_snap kept but threshold raised to 70 (0/3 in new test)
        "allowed_strategies": {
            "ema_micro_cross",         # 69.0% WR (29 trades) ← NEW — best performer
            "three_candle_reversal",   # 60.9% WR (23 trades)
            "stoch_snap",              # 0/3 in new test → threshold raised to 70
            "otc_trend_confirm",       # 62.5% WR (prior data)
        },
        # Sessions where a strategy must NOT fire
        "strategy_session_blocks": {
            "three_candle_reversal": {"BULL"},   # 40% WR in BULL session
        },
        # Per-strategy minimum confidence
        # stoch_snap raised: 0W/3L in latest 102-trade test
        # double_bottom_top: 53.8% WR — raise bar even though not in allowed list
        "strategy_min_conf": {
            "ema_micro_cross":         62,   # new — matches global floor
            "three_candle_reversal":   62,   # raised from 58 (conf 65-70% = 50% WR)
            "stoch_snap":              70,   # raised from 55 (0W/3L → need high confidence)
            "otc_trend_confirm":       62,   # raised from 55
            "double_bottom_top":       68,   # 53.8% WR → require high confidence even for paper
        },
        # Per-pair minimum confidence
        "pair_min_conf": {
            "EUR/CHF OTC":  62,
            "USD/CAD OTC":  62,
            "AUD/CHF OTC":  65,
            "AUD/CAD OTC":  68,
            "AUD/USD OTC":  65,
        },
        # Historical WR used as base for confidence recalculation
        "strategy_wr": {
            "ema_micro_cross":         69.0,  # 29 trades, Apr 2026
            "three_candle_reversal":   60.9,  # 23 trades, Apr 2026
            "stoch_snap":              70.0,  # prior data — keep optimistic but threshold enforced
            "otc_trend_confirm":       62.5,  # prior data
            "double_bottom_top":       53.8,  # 13 trades — honest calibration
        },
        # Pair multipliers for confidence recalc
        "pair_multiplier": {
            "EUR/CHF OTC":  1.10,   # 100% WR
            "USD/CAD OTC":  1.00,   # neutral
            "AUD/CHF OTC":  0.90,   # downgraded — 40% WR in new test
            "AUD/CAD OTC":  0.85,   # downgraded — 25% WR
            "AUD/USD OTC":  0.90,
        },
        # Session multipliers
        "session_multiplier": {
            "BEAR":    1.05,
            "BULL":    1.00,
            "NEUTRAL": 0.95,
        },
        # Rate limits
        "max_simultaneous":    3,     # active trades at one time
        "min_time_between":   30,     # seconds between any two signals
        "hourly_pair_limit": {        # max trades per pair per rolling hour
            "AUD/CHF OTC": 3,
            "EUR/CHF OTC": 3,
            "USD/CAD OTC": 3,
            "AUD/CAD OTC": 2,
            "AUD/USD OTC": 2,
        },
        "hourly_total_limit":  15,    # max total trades per rolling hour
        "daily_limit":        100,    # max total trades per calendar day
        "loss_streak_max":      3,    # consecutive losses before pause
        "loss_streak_pause":  600,    # pause duration in seconds (10 min)
        "direction_streak_max": 5,    # max same-direction trades in a row

        # Raised from 55: bucket 55-60% and 60-65% had similar WR (61%), but
        # bucket 65-70% had 50% WR (coin flip) — calibration is broken there.
        # New floor forces signals to be either high-confidence (70+) or skip.
        "global_min_confidence": 62,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SignalFilter
# ─────────────────────────────────────────────────────────────────────────────

class SignalFilter:
    """
    Pass a signal dict through 15 ordered filters.
    Stateful — maintains active trade list, history, and streak counters.

    Signal dict keys expected by check():
        pair          str   — e.g. "AUD/CHF OTC"
        direction     str   — "BUY" | "SELL"
        strategy      str   — e.g. "three_candle_reversal"
        expiry        str   — "1m" | "2m"
        confidence    float — raw engine confidence 0-100
        session       str   — "BULL" | "BEAR" | "NEUTRAL"
        entry_price   float — current market price
        entry_time    float — unix timestamp (time.time())
    """

    def __init__(self, config: Optional[dict] = None):
        self.config = _default_config()
        if config:
            self.config.update(config)

        # Active trades: list of {"pair", "expiry_sec", "start_time"}
        self._active: list[dict] = []

        # Timestamped trade log for rate-limit windows (rolling deque — 24h)
        self._history: deque[dict] = deque()

        # Per-pair consecutive loss counter
        self._pair_loss_streak: dict[str, int] = {}

        # Global consecutive loss counter (any pair)
        self._global_loss_streak: int = 0
        self._loss_pause_until: float = 0.0

        # Consecutive direction tracker
        self._direction_streak: list[str] = []   # last N directions sent

        # Daily counter (date string → count)
        self._daily_date: str = ""
        self._daily_count: int = 0

        # Last trade time (global)
        self._last_trade_time: float = 0.0

    # ── Public API ────────────────────────────────────────────────────────────

    def check(self, signal: dict) -> dict:
        """
        Run signal through all 15 filters.
        Returns a result dict with approved/reason/new_confidence etc.
        """
        self._cleanup_active_trades()
        self._cleanup_old_history()

        passed: list[str] = []
        failed: list[str] = []

        pair      = signal.get("pair", "")
        direction = signal.get("direction", "")
        strategy  = signal.get("strategy", "")
        expiry    = signal.get("expiry", "1m")
        confidence = float(signal.get("confidence", 0))
        session   = signal.get("session", "NEUTRAL")
        entry_time = float(signal.get("entry_time", time.time()))

        expiry_sec = 120 if expiry == "2m" else 60

        def _reject(filter_name: str, reason: str) -> dict:
            failed.append(filter_name)
            return {
                "approved": False,
                "reason": reason,
                "signal": dict(signal),
                "new_confidence": 0.0,
                "filters_passed": passed,
                "filters_failed": failed,
            }

        # ── Filter 1: PAIR_BLACKLIST ───────────────────────────────────────────
        if pair in self.config["blacklisted_pairs"]:
            return _reject("PAIR_BLACKLIST",
                           f"Пара {pair} в чёрном списке (WR < 50%)")
        passed.append("PAIR_BLACKLIST")

        # ── Filter 2: PAIR_WHITELIST ───────────────────────────────────────────
        in_white = pair in self.config["whitelisted_pairs"]
        in_grey  = pair in self.config["grey_pairs"]
        if not in_white and not in_grey:
            return _reject("PAIR_WHITELIST",
                           f"Пара {pair} не в белом/сером списке")
        passed.append("PAIR_WHITELIST")

        # ── Filter 3: STRATEGY_ENABLED ────────────────────────────────────────
        if strategy not in self.config["allowed_strategies"]:
            return _reject("STRATEGY_ENABLED",
                           f"Стратегия {strategy} не разрешена для отправки")
        passed.append("STRATEGY_ENABLED")

        # ── Filter 4: STRATEGY_SESSION ────────────────────────────────────────
        blocked_sessions = self.config["strategy_session_blocks"].get(strategy, set())
        if session in blocked_sessions:
            return _reject("STRATEGY_SESSION",
                           f"{strategy} заблокирована в сессии {session}")
        passed.append("STRATEGY_SESSION")

        # ── Filter 5: STRATEGY_EXPIRY ─────────────────────────────────────────
        _ONE_MIN  = {"three_candle_reversal", "stoch_snap"}
        _TWO_MIN  = {"otc_trend_confirm"}
        if strategy in _ONE_MIN and expiry != "1m":
            return _reject("STRATEGY_EXPIRY",
                           f"{strategy} только 1m, получен {expiry}")
        if strategy in _TWO_MIN and expiry != "2m":
            return _reject("STRATEGY_EXPIRY",
                           f"{strategy} только 2m, получен {expiry}")
        passed.append("STRATEGY_EXPIRY")

        # ── Filter 6: CONFIDENCE_GLOBAL ───────────────────────────────────────
        min_global = self.config["global_min_confidence"]
        if confidence < min_global:
            return _reject("CONFIDENCE_GLOBAL",
                           f"Уверенность {confidence:.0f} < глобальный минимум {min_global}")
        passed.append("CONFIDENCE_GLOBAL")

        # ── Filter 7: CONFIDENCE_STRATEGY ────────────────────────────────────
        min_strat = self.config["strategy_min_conf"].get(strategy, min_global)
        if confidence < min_strat:
            return _reject("CONFIDENCE_STRATEGY",
                           f"Уверенность {confidence:.0f} < минимум стратегии {min_strat}")
        passed.append("CONFIDENCE_STRATEGY")

        # ── Filter 8: CONFIDENCE_PAIR ─────────────────────────────────────────
        min_pair = self.config["pair_min_conf"].get(pair, min_global)
        if confidence < min_pair:
            return _reject("CONFIDENCE_PAIR",
                           f"Уверенность {confidence:.0f} < минимум пары {min_pair}")
        passed.append("CONFIDENCE_PAIR")

        # ── Filter 9: MAX_SIMULTANEOUS ────────────────────────────────────────
        if len(self._active) >= self.config["max_simultaneous"]:
            return _reject("MAX_SIMULTANEOUS",
                           f"Уже {len(self._active)} активных сделок (макс {self.config['max_simultaneous']})")
        passed.append("MAX_SIMULTANEOUS")

        # ── Filter 10: MIN_TIME_BETWEEN ───────────────────────────────────────
        min_gap = self.config["min_time_between"]
        gap = entry_time - self._last_trade_time
        if self._last_trade_time > 0 and gap < min_gap:
            return _reject("MIN_TIME_BETWEEN",
                           f"Прошло {gap:.0f}с с последней сделки (мин {min_gap}с)")
        passed.append("MIN_TIME_BETWEEN")

        # ── Filter 11: HOURLY_PAIR_LIMIT ─────────────────────────────────────
        hour_ago = entry_time - 3600
        pair_hour = sum(1 for h in self._history
                        if h["pair"] == pair and h["time"] >= hour_ago)
        pair_limit = self.config["hourly_pair_limit"].get(pair, 99)
        if pair_hour >= pair_limit:
            return _reject("HOURLY_PAIR_LIMIT",
                           f"{pair}: {pair_hour} сделок в час (лимит {pair_limit})")
        passed.append("HOURLY_PAIR_LIMIT")

        # ── Filter 12: HOURLY_TOTAL_LIMIT ────────────────────────────────────
        total_hour = sum(1 for h in self._history if h["time"] >= hour_ago)
        if total_hour >= self.config["hourly_total_limit"]:
            return _reject("HOURLY_TOTAL_LIMIT",
                           f"{total_hour} сделок в час (лимит {self.config['hourly_total_limit']})")
        passed.append("HOURLY_TOTAL_LIMIT")

        # ── Filter 13: DAILY_LIMIT ────────────────────────────────────────────
        today = _today_str()
        if self._daily_date != today:
            self._daily_date  = today
            self._daily_count = 0
        if self._daily_count >= self.config["daily_limit"]:
            return _reject("DAILY_LIMIT",
                           f"{self._daily_count} сделок сегодня (лимит {self.config['daily_limit']})")
        passed.append("DAILY_LIMIT")

        # ── Filter 14: LOSS_STREAK ────────────────────────────────────────────
        if entry_time < self._loss_pause_until:
            remaining = int(self._loss_pause_until - entry_time)
            return _reject("LOSS_STREAK",
                           f"Пауза после {self.config['loss_streak_max']} потерь подряд — ещё {remaining}с")
        passed.append("LOSS_STREAK")

        # ── Filter 15: DIRECTION_BALANCE ─────────────────────────────────────
        max_streak = self.config["direction_streak_max"]
        if len(self._direction_streak) >= max_streak:
            if all(d == direction for d in self._direction_streak[-max_streak:]):
                return _reject("DIRECTION_BALANCE",
                               f"{max_streak} сделок подряд в направлении {direction} — пропускаем")
        passed.append("DIRECTION_BALANCE")

        # ── All filters passed — recalculate confidence ───────────────────────
        new_conf = self._recalculate_confidence(signal)
        updated_signal = {**signal, "confidence": new_conf}

        # Register the trade
        self._register_trade(updated_signal, expiry_sec, entry_time)

        logger.info(
            "✅ Signal APPROVED: %s %s | strategy=%s | conf %.0f→%.0f | session=%s | filters=%d/15",
            pair, direction, strategy, confidence, new_conf, session, len(passed)
        )

        return {
            "approved": True,
            "reason": "Все фильтры пройдены",
            "signal": updated_signal,
            "new_confidence": new_conf,
            "filters_passed": passed,
            "filters_failed": [],
        }

    def update_result(self, pair: str, result: str) -> None:
        """
        Update WIN/LOSS tracking after trade closes.
        result: "WIN" | "LOSS" | "DRAW"
        """
        if result == "LOSS":
            self._pair_loss_streak[pair] = self._pair_loss_streak.get(pair, 0) + 1
            self._global_loss_streak += 1
            if self._global_loss_streak >= self.config["loss_streak_max"]:
                self._loss_pause_until = time.time() + self.config["loss_streak_pause"]
                logger.warning(
                    "🛑 Loss streak %d — pausing signals for %ds",
                    self._global_loss_streak, self.config["loss_streak_pause"]
                )
        else:
            self._pair_loss_streak[pair] = 0
            self._global_loss_streak = 0

        logger.info("Filter result update: %s → %s (pair_streak=%d, global_streak=%d)",
                    pair, result,
                    self._pair_loss_streak.get(pair, 0),
                    self._global_loss_streak)

    def reset_daily(self) -> None:
        """Reset daily counters (call at midnight)."""
        self._daily_count = 0
        self._daily_date  = _today_str()

    def get_stats(self) -> dict:
        now = time.time()
        hour_ago = now - 3600
        today = _today_str()
        if self._daily_date != today:
            self._daily_count = 0
        return {
            "active_trades":     len(self._active),
            "trades_today":      self._daily_count,
            "trades_last_hour":  sum(1 for h in self._history if h["time"] >= hour_ago),
            "global_loss_streak": self._global_loss_streak,
            "loss_pause_until":  self._loss_pause_until,
            "loss_paused":       now < self._loss_pause_until,
            "last_trade_time":   self._last_trade_time,
        }

    # ── Internal ─────────────────────────────────────────────────────────────

    def _register_trade(self, signal: dict, expiry_sec: int, entry_time: float) -> None:
        self._active.append({
            "pair":       signal["pair"],
            "expiry_sec": expiry_sec,
            "start_time": entry_time,
        })
        self._history.append({
            "pair":      signal["pair"],
            "direction": signal["direction"],
            "time":      entry_time,
        })
        self._direction_streak.append(signal["direction"])
        if len(self._direction_streak) > 10:
            self._direction_streak.pop(0)
        self._last_trade_time = entry_time
        self._daily_count += 1

    def _cleanup_active_trades(self) -> None:
        now = time.time()
        self._active = [
            t for t in self._active
            if now - t["start_time"] < t["expiry_sec"] + 5
        ]

    def _cleanup_old_history(self) -> None:
        cutoff = time.time() - 86400  # keep 24h
        while self._history and self._history[0]["time"] < cutoff:
            self._history.popleft()

    def _recalculate_confidence(self, signal: dict) -> float:
        """
        new_conf = (base_wr * pair_mult * session_mult) * 0.80
                   + (original_conf * 0.20)
                   + combo_bonus
                   - pair_loss_penalty
        Clamped 0-100.
        """
        strategy   = signal.get("strategy", "")
        pair       = signal.get("pair", "")
        session    = signal.get("session", "NEUTRAL")
        direction  = signal.get("direction", "")
        orig_conf  = float(signal.get("confidence", 55))

        base_wr    = self.config["strategy_wr"].get(strategy, 55.0)
        pair_mult  = self.config["pair_multiplier"].get(pair, 1.0)
        sess_mult  = self.config["session_multiplier"].get(session, 1.0)

        # Weighted blend: 80% historical WR component, 20% engine score
        new_conf   = (base_wr * pair_mult * sess_mult) * 0.80 + orig_conf * 0.20

        # Combo bonuses
        combo = 0.0
        if strategy == "three_candle_reversal" and session == "BEAR":
            combo += 5.0   # best combo: 69.2% WR in BEAR
        if pair == "EUR/CHF OTC" and strategy in ("ema_micro_cross", "three_candle_reversal"):
            combo += 3.0   # EUR/CHF: 100% WR in new test

        new_conf += combo

        # Pair loss streak penalty
        pair_losses = self._pair_loss_streak.get(pair, 0)
        if pair_losses >= 2:
            new_conf -= 10.0

        return round(max(0.0, min(100.0, new_conf)), 1)


def _today_str() -> str:
    import datetime
    return datetime.date.today().isoformat()
