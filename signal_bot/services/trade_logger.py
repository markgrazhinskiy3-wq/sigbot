"""
TradeLogger — CSV logging for approved and rejected signals.

trades_log.csv   — every signal that passed all 15 filters
rejected_log.csv — every signal that was blocked (for analysis)
"""
from __future__ import annotations

import csv
import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_TRADES_HEADERS = [
    "timestamp", "pair", "direction", "strategy", "expiry", "session",
    "original_confidence", "new_confidence", "entry_price",
    "close_price", "result", "pnl_pct", "filters_passed",
]

_REJECTED_HEADERS = [
    "timestamp", "pair", "direction", "strategy", "expiry", "session",
    "confidence", "reason", "failed_filter",
]


class TradeLogger:
    """
    Append-only CSV logger for signal outcomes.

    Usage:
        logger = TradeLogger("trades.csv", "rejected.csv")
        logger.log_trade(filter_result)          # after approval
        logger.log_rejected(filter_result)       # after rejection
        logger.update_trade_result("AUD/CHF OTC", "WIN", 92.0, 1.23456)
    """

    def __init__(
        self,
        trades_path: str = "trades_log.csv",
        rejected_path: str = "rejected_log.csv",
    ):
        self.trades_path   = trades_path
        self.rejected_path = rejected_path
        self._ensure_file(trades_path,   _TRADES_HEADERS)
        self._ensure_file(rejected_path, _REJECTED_HEADERS)

    # ── Public ────────────────────────────────────────────────────────────────

    def log_trade(self, signal_result: dict) -> None:
        """Log an approved signal as PENDING."""
        sig        = signal_result.get("signal", {})
        new_conf   = signal_result.get("new_confidence", 0)
        n_passed   = len(signal_result.get("filters_passed", []))
        row = {
            "timestamp":           _now(),
            "pair":                sig.get("pair", ""),
            "direction":           sig.get("direction", ""),
            "strategy":            sig.get("strategy", ""),
            "expiry":              sig.get("expiry", ""),
            "session":             sig.get("session", ""),
            "original_confidence": round(float(sig.get("confidence", 0)), 1),
            "new_confidence":      round(float(new_conf), 1),
            "entry_price":         sig.get("entry_price", ""),
            "close_price":         "",
            "result":              "PENDING",
            "pnl_pct":             "",
            "filters_passed":      n_passed,
        }
        self._append(self.trades_path, _TRADES_HEADERS, row)
        logger.debug("TradeLogger: logged approved trade %s %s", row["pair"], row["direction"])

    def log_rejected(self, signal_result: dict) -> None:
        """Log a rejected signal with failure reason."""
        sig         = signal_result.get("signal", {})
        failed      = signal_result.get("filters_failed", [])
        reason      = signal_result.get("reason", "")
        failed_name = failed[0] if failed else ""
        row = {
            "timestamp":    _now(),
            "pair":         sig.get("pair", ""),
            "direction":    sig.get("direction", ""),
            "strategy":     sig.get("strategy", ""),
            "expiry":       sig.get("expiry", ""),
            "session":      sig.get("session", ""),
            "confidence":   round(float(sig.get("confidence", 0)), 1),
            "reason":       reason,
            "failed_filter": failed_name,
        }
        self._append(self.rejected_path, _REJECTED_HEADERS, row)

    def update_trade_result(
        self,
        pair: str,
        result: str,
        pnl: Optional[float] = None,
        close_price: Optional[float] = None,
    ) -> None:
        """
        Find the last PENDING row for this pair and update result/pnl/close_price.
        Rewrites the file in-place — safe for small CSV sizes.
        """
        if not os.path.exists(self.trades_path):
            return

        rows = []
        updated = False
        try:
            with open(self.trades_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            # Find last PENDING row for this pair (search from end)
            for row in reversed(rows):
                if row.get("pair") == pair and row.get("result") == "PENDING":
                    row["result"]      = result
                    row["pnl_pct"]     = round(pnl, 2)     if pnl         is not None else ""
                    row["close_price"] = round(close_price, 6) if close_price is not None else ""
                    updated = True
                    break

            if not updated:
                logger.debug("TradeLogger: no PENDING row found for %s", pair)
                return

            with open(self.trades_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=_TRADES_HEADERS)
                writer.writeheader()
                writer.writerows(rows)

            logger.debug("TradeLogger: updated result for %s → %s", pair, result)

        except Exception as exc:
            logger.warning("TradeLogger: failed to update result for %s: %s", pair, exc)

    # ── Internal ─────────────────────────────────────────────────────────────

    @staticmethod
    def _ensure_file(path: str, headers: list[str]) -> None:
        """Create CSV file with headers if it doesn't exist."""
        if not os.path.exists(path):
            try:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                logger.info("TradeLogger: created %s", path)
            except Exception as exc:
                logger.warning("TradeLogger: could not create %s: %s", path, exc)

    @staticmethod
    def _append(path: str, headers: list[str], row: dict) -> None:
        try:
            with open(path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writerow(row)
        except Exception as exc:
            logger.warning("TradeLogger: failed to append to %s: %s", path, exc)


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
