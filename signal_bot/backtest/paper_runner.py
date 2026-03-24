"""
Paper Trading Runner
====================
Silently scans OTC pairs in real-time using the IDENTICAL decision engine as
live trading. NO messages are sent to users. Trades are logged to signals_log
with source='paper'.

Design (no lookahead bias):
  1. Fetch fresh 15s candles for all pairs via WS
  2. Run calculate_signal() — identical to live
  3. If BUY/SELL → record entry_price from the LAST available candle
  4. Sleep expiry_sec (1m or 2m) — time passes, market moves
  5. Fetch new candles → take LAST close as result_price
  6. WIN if result moved in signal direction, else LOSS
  7. Log to signals_log (source='paper')
  8. Repeat until target_trades collected

Usage from bot:  /paper_test [n]     (n defaults to 100)
Usage standalone: python -m backtest.paper_runner --target 100
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Awaitable

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config
from services.strategy_engine import calculate_signal
from services.candle_cache import resample_to_1m, resample_to_5m
from services import analytics_logger

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

# How often to re-scan all pairs (seconds). Keep ≥ 45s to avoid WS overload.
SCAN_INTERVAL: float  = 50.0

# Pairs to scan. Use all configured OTC pairs by default.
_DEFAULT_PAIRS: list[dict] = config.OTC_PAIRS

# Signal cooldown per pair (seconds) — don't fire a new signal on the same pair
# within this window (prevents signal clustering on one pair).
COOLDOWN_PER_PAIR: float = 180.0   # v5: raised from 120s — prevents duplicate entries

# Minimum candles required before running the engine
MIN_CANDLES: int = 60


# ── Dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class PaperTrade:
    symbol:     str
    pair:       str
    direction:  str
    expiry:     str           # "1m" | "2m"
    expiry_sec: int
    entry_price: float
    entry_time:  float        # unix timestamp
    details:     dict
    outcome_id:  int | None = None  # analytics FK (None for paper)
    logged_at:   float = field(default_factory=time.time)


@dataclass
class TradeResult:
    trade:        PaperTrade
    close_price:  float
    result:       str           # "WIN" | "LOSS"
    pnl_pct:      float         # signed: positive = direction correct


# ── Progress callback type ───────────────────────────────────────────────────

ProgressCb = Callable[[str], Awaitable[None]] | None


# ── Core runner ───────────────────────────────────────────────────────────────

class PaperRunner:
    """
    Runs the paper trading simulation until `target_trades` are resolved.
    All signal generation uses the real engine — no shortcuts.
    """

    def __init__(
        self,
        target_trades: int = 100,
        pairs: list[dict] | None = None,
        expiry: str = "both",            # "1m" | "2m" | "both"
        progress_cb: ProgressCb = None,
        progress_every: int = 10,
    ):
        self.target_trades  = target_trades
        self.pairs          = pairs or _DEFAULT_PAIRS
        self.expiry         = expiry
        self.progress_cb    = progress_cb
        self.progress_every = progress_every

        self._active:   dict[str, PaperTrade]  = {}   # symbol → pending trade
        self._results:  list[TradeResult]       = []
        self._cooldown: dict[str, float]        = {}   # symbol → last_fire_ts
        self._cancelled = False

    @property
    def resolved_count(self) -> int:
        return len(self._results)

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self) -> list[TradeResult]:
        """
        Main entry point. Returns list of resolved TradeResult when done.
        """
        logger.info(
            "PaperRunner START: target=%d pairs=%d expiry=%s",
            self.target_trades, len(self.pairs), self.expiry,
        )

        await analytics_logger.init_analytics()

        # ── Wait for candle cache to warm up before scanning ──────────────────
        logger.info("PaperRunner: waiting for candle cache to populate…")
        for _wait_attempt in range(24):   # max 2 minutes
            if self._cancelled:
                return self._results
            probe = self._from_cache([self.pairs[0]["symbol"]])
            if probe:
                logger.info("PaperRunner: cache ready — starting scan")
                break
            logger.info(
                "PaperRunner: cache not ready yet (attempt %d/24), waiting 5s…",
                _wait_attempt + 1,
            )
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                self._cancelled = True
                return self._results

        scan_num = 0
        while self.resolved_count < self.target_trades and not self._cancelled:
            scan_num += 1
            t0 = time.time()

            # 1. Fetch candles for all pairs in one WS call
            candles_map = await self._fetch_candles()
            if not candles_map:
                logger.warning("PaperRunner: no candles fetched — waiting %ds", int(SCAN_INTERVAL))
                await asyncio.sleep(SCAN_INTERVAL)
                continue

            # 2. Check active trades that have expired
            await self._resolve_expired(candles_map)
            if self.resolved_count >= self.target_trades:
                break

            # 3. Scan for new signals on pairs without active trades
            await self._scan_signals(candles_map)

            elapsed = time.time() - t0
            wait = max(0.0, SCAN_INTERVAL - elapsed)
            logger.info(
                "PaperRunner scan #%d: resolved=%d/%d active=%d waiting=%.1fs",
                scan_num, self.resolved_count, self.target_trades,
                len(self._active), wait,
            )

            if self.resolved_count < self.target_trades:
                try:
                    await asyncio.sleep(wait)
                except asyncio.CancelledError:
                    self._cancelled = True
                    break

        logger.info("PaperRunner DONE: %d trades resolved", self.resolved_count)
        return self._results

    # ── Candle fetch ──────────────────────────────────────────────────────────

    async def _fetch_candles(self) -> dict[str, list[dict]]:
        """
        Get candles for all pairs.
        Priority: shared in-memory cache (always fresh from live bot's refresher)
        then WS fetch as supplement for pairs missing from cache.
        """
        symbols = [p["symbol"] for p in self.pairs]

        # ── Primary: shared candle cache (maintained by live bot's refresher) ─
        cached = self._from_cache(symbols)
        if cached:
            return cached

        # ── Fallback: direct WS fetch (slower, opens new connection) ──────────
        logger.info("PaperRunner: cache empty — trying WS fetch directly")
        try:
            from services.po_ws_client import fetch_all_pairs, is_available
            if not is_available():
                logger.warning("PaperRunner: WS also not available")
                return {}

            async with asyncio.timeout(60):
                result = await fetch_all_pairs(symbols)

            filtered = {
                sym: c for sym, c in result.items()
                if len(c) >= MIN_CANDLES
            }
            if filtered:
                logger.info("PaperRunner: WS fetch got %d pairs", len(filtered))
            return filtered
        except Exception as exc:
            logger.warning("PaperRunner: WS fetch also failed: %s", exc)
            return {}

    def _from_cache(self, symbols: list[str]) -> dict[str, list[dict]]:
        """
        Fallback: read directly from shared in-memory candle cache.
        Bypasses TTL check — even slightly stale candles are valid for signal generation.
        """
        try:
            from services.candle_cache import _cache as _raw_cache
            result = {}
            for sym in symbols:
                entry = _raw_cache.get(sym)
                if entry and len(entry.candles) >= MIN_CANDLES:
                    result[sym] = list(entry.candles)
            if result:
                logger.debug("PaperRunner: _from_cache: got %d pairs", len(result))
            return result
        except Exception as exc:
            logger.debug("PaperRunner: _from_cache failed: %s", exc)
            return {}

    # ── Signal scanning ───────────────────────────────────────────────────────

    async def _scan_signals(self, candles_map: dict[str, list[dict]]) -> None:
        """Run the signal engine on each idle pair and record any signals fired."""
        now = time.time()

        # Pick expiry for this scan
        expiry_modes = []
        if self.expiry == "both":
            expiry_modes = ["1m", "2m"]
        else:
            expiry_modes = [self.expiry]

        for p in self.pairs:
            sym   = p["symbol"]
            label = p["label"]

            # Skip if pair has an active trade
            if sym in self._active:
                continue

            # Skip if pair is in cooldown
            if now - self._cooldown.get(sym, 0) < COOLDOWN_PER_PAIR:
                continue

            candles = candles_map.get(sym)
            if not candles:
                continue

            # Try each expiry and take the first signal
            for exp_str in expiry_modes:
                if self.resolved_count + len(self._active) >= self.target_trades * 2:
                    # Don't queue too many trades at once
                    break

                try:
                    result = await calculate_signal(candles, expiry=exp_str)
                except Exception as exc:
                    logger.debug("PaperRunner: calc error %s %s: %s", sym, exp_str, exc)
                    continue

                if result.direction not in ("BUY", "SELL"):
                    continue

                # Signal found!
                d            = result.details if isinstance(result.details, dict) else {}
                entry_price  = d.get("debug", {}).get("last_close")
                if not entry_price:
                    continue

                exp_sec = 120 if exp_str == "2m" else 60
                trade = PaperTrade(
                    symbol      = sym,
                    pair        = label,
                    direction   = result.direction,
                    expiry      = exp_str,
                    expiry_sec  = exp_sec,
                    entry_price = entry_price,
                    entry_time  = now,
                    details     = d,
                )
                self._active[sym] = trade
                self._cooldown[sym] = now

                # Log to analytics with source='paper'
                asyncio.create_task(
                    analytics_logger.log_signal(
                        outcome_id=None,
                        pair=label,
                        symbol=sym,
                        direction=result.direction,
                        expiry=exp_str,
                        entry_price=entry_price,
                        details=d,
                        source="paper",
                    )
                )

                logger.info(
                    "PaperRunner: SIGNAL %s %s %s entry=%.6f score=%.1f",
                    label, result.direction, exp_str, entry_price,
                    d.get("debug", {}).get("final_score") or 0,
                )
                break  # one signal per pair per scan

    # ── Expiry resolution ─────────────────────────────────────────────────────

    async def _resolve_expired(self, candles_map: dict[str, list[dict]]) -> None:
        """Check active trades. Resolve those whose expiry time has passed."""
        now = time.time()
        to_resolve = [
            trade for trade in self._active.values()
            if now - trade.entry_time >= trade.expiry_sec
        ]

        for trade in to_resolve:
            sym     = trade.symbol
            candles = candles_map.get(sym)
            if not candles:
                # Can't resolve yet — candles unavailable
                logger.debug("PaperRunner: no candles to resolve %s, postponing", sym)
                continue

            close_price = float(candles[-1]["close"])

            if trade.direction == "BUY":
                pnl_pct = (close_price - trade.entry_price) / trade.entry_price * 100
                if close_price == trade.entry_price:
                    outcome = "draw"
                else:
                    outcome = "win" if close_price > trade.entry_price else "loss"
            else:
                pnl_pct = (trade.entry_price - close_price) / trade.entry_price * 100
                if close_price == trade.entry_price:
                    outcome = "draw"
                else:
                    outcome = "win" if close_price < trade.entry_price else "loss"

            pnl_signed = 0.0 if outcome == "draw" else (pnl_pct if outcome == "win" else -abs(pnl_pct))

            res = TradeResult(
                trade       = trade,
                close_price = close_price,
                result      = outcome.upper(),
                pnl_pct     = round(pnl_signed, 5),
            )
            self._results.append(res)
            del self._active[sym]

            # Update analytics record (by pair+timestamp since we used outcome_id=None)
            asyncio.create_task(
                analytics_logger.update_result_by_paper(
                    pair       = trade.pair,
                    symbol     = sym,
                    entry_price = trade.entry_price,
                    close_price = close_price,
                    result      = outcome,
                    pnl_pct     = round(pnl_signed, 5),
                )
            )

            logger.info(
                "PaperRunner: RESULT %s %s → %s entry=%.6f close=%.6f pnl=%.4f%%",
                trade.pair, trade.direction, outcome.upper(),
                trade.entry_price, close_price, pnl_pct,
            )

            # Progress update
            n = self.resolved_count
            if self.progress_cb and n % self.progress_every == 0 and n > 0:
                asyncio.create_task(self.progress_cb(
                    self._progress_text(n)
                ))

    # ── Summary & progress ────────────────────────────────────────────────────

    def _progress_text(self, n: int) -> str:
        wins   = sum(1 for r in self._results if r.result == "WIN")
        draws  = sum(1 for r in self._results if r.result == "DRAW")
        decisive = n - draws
        wr = round(wins / decisive * 100, 1) if decisive else 0
        draw_note = f" | DRAW {draws}" if draws else ""
        return f"📊 Paper test progress: {n}/{self.target_trades} trades | WR {wr}%{draw_note}"

    def build_summary(self) -> str:
        """Build full summary report string."""
        results = self._results
        if not results:
            return "❌ No trades resolved yet."

        n      = len(results)
        wins   = sum(1 for r in results if r.result == "WIN")
        draws  = sum(1 for r in results if r.result == "DRAW")
        losses = n - wins - draws
        decisive = n - draws
        wr     = round(wins / decisive * 100, 1) if decisive else 0.0
        avg_pnl = round(sum(r.pnl_pct for r in results) / n, 4)

        lines = [
            "=" * 50,
            "📊  PAPER TRADING SUMMARY",
            "=" * 50,
            "",
            f"Total trades:    {n}",
            f"WIN / LOSS / DRAW: {wins} / {losses} / {draws}",
            f"Overall winrate: {wr}%  (excl. draws)",
            f"Avg pnl/trade:   {avg_pnl:+.4f}%",
            "",
        ]

        # ── By pattern ──────────────────────────────────────────────────────
        lines.append("── Winrate by pattern ──")
        pat_map: dict[str, list[TradeResult]] = {}
        for r in results:
            pat = r.trade.details.get("primary_strategy") or "unknown"
            pat_map.setdefault(pat, []).append(r)
        for pat, rs in sorted(pat_map.items(), key=lambda x: -len(x[1])):
            pw = sum(1 for r in rs if r.result == "WIN")
            pwr = round(pw / len(rs) * 100, 1)
            bar = _bar(pwr)
            lines.append(f"  {pat:<25} {pw:>3}/{len(rs):<3}  {pwr:>5.1f}%  {bar}")

        lines.append("")

        # ── By expiry ────────────────────────────────────────────────────────
        lines.append("── Winrate by expiry ──")
        exp_map: dict[str, list[TradeResult]] = {}
        for r in results:
            exp_map.setdefault(r.trade.expiry, []).append(r)
        for exp, rs in sorted(exp_map.items()):
            ew = sum(1 for r in rs if r.result == "WIN")
            ewr = round(ew / len(rs) * 100, 1)
            bar = _bar(ewr)
            lines.append(f"  {exp:<6}  {ew:>3}/{len(rs):<3}  {ewr:>5.1f}%  {bar}")

        lines.append("")

        # ── By score bucket ──────────────────────────────────────────────────
        lines.append("── Winrate by score bucket ──")
        buckets = [
            ("65–70", 65, 70), ("70–75", 70, 75), ("75–80", 75, 80),
            ("80–85", 80, 85), ("85+",   85, 999),
        ]
        for label, lo, hi in buckets:
            rs = [
                r for r in results
                if lo <= (r.trade.details.get("debug", {}).get("final_score") or 0) < hi
            ]
            if not rs:
                continue
            bw  = sum(1 for r in rs if r.result == "WIN")
            bwr = round(bw / len(rs) * 100, 1)
            bar = _bar(bwr)
            lines.append(f"  {label:<8}  {bw:>3}/{len(rs):<3}  {bwr:>5.1f}%  {bar}")

        lines.append("")

        # ── By pair ──────────────────────────────────────────────────────────
        lines.append("── Winrate by pair ──")
        pair_map: dict[str, list[TradeResult]] = {}
        for r in results:
            pair_map.setdefault(r.trade.pair, []).append(r)
        for pair, rs in sorted(pair_map.items(), key=lambda x: -len(x[1])):
            pw = sum(1 for r in rs if r.result == "WIN")
            pwr = round(pw / len(rs) * 100, 1)
            lines.append(f"  {pair:<20}  {pw}/{len(rs)}  {pwr}%")

        lines.append("")

        # ── Pattern distribution ─────────────────────────────────────────────
        lines.append("── Pattern frequency ──")
        for pat, rs in sorted(pat_map.items(), key=lambda x: -len(x[1])):
            pct = round(len(rs) / n * 100, 1)
            bar = _bar(pct)
            lines.append(f"  {pat:<25} {len(rs):>3}  ({pct}%)  {bar}")

        lines += ["", "=" * 50]
        return "\n".join(lines)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bar(pct: float, width: int = 8) -> str:
    """ASCII progress bar 0-100%."""
    filled = round(pct / 100 * width)
    return "█" * filled + "░" * (width - filled)


# ── Public API ────────────────────────────────────────────────────────────────

async def run_paper_test(
    target:      int = 100,
    expiry:      str = "both",
    pairs:       list[dict] | None = None,
    progress_cb: ProgressCb = None,
    progress_every: int = 10,
) -> tuple[list[TradeResult], str]:
    """
    Run paper trading until `target` trades are resolved.

    Returns:
        (results, summary_text)
    """
    runner = PaperRunner(
        target_trades  = target,
        pairs          = pairs,
        expiry         = expiry,
        progress_cb    = progress_cb,
        progress_every = progress_every,
    )
    results = await runner.run()
    summary = runner.build_summary()
    return results, summary


# ── Standalone CLI ────────────────────────────────────────────────────────────

async def _cli_main():
    import argparse
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    ap = argparse.ArgumentParser(description="Paper Trading Runner")
    ap.add_argument("--target", type=int, default=100, help="Target trade count")
    ap.add_argument("--expiry", choices=["1m", "2m", "both"], default="both")
    args = ap.parse_args()

    from db.database import init_db
    await init_db()

    async def _print_progress(msg: str):
        print(msg)

    print(f"Starting paper test: target={args.target} expiry={args.expiry}")
    results, summary = await run_paper_test(
        target      = args.target,
        expiry      = args.expiry,
        progress_cb = _print_progress,
    )
    print(summary)


if __name__ == "__main__":
    asyncio.run(_cli_main())
