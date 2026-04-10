"""
SignalFormatter — formats approved/rejected signals and results as Telegram HTML.

Separate from signal_service.py because it works specifically with the
post-filter signal dict (new_confidence, filters_passed count, etc.)
"""
from __future__ import annotations

import datetime
from typing import Optional


_STRATEGY_NAMES = {
    "three_candle_reversal": "3 свечи (разворот)",
    "stoch_snap":            "Stochastic Snap",
    "otc_trend_confirm":     "MACD+RSI тренд",
    "rsi_bb_scalp":          "RSI + Bollinger",
    "ema_micro_cross":       "EMA Micro-Cross",
    "double_bottom_top":     "Двойное дно/вершина",
}


class SignalFormatter:
    """Format filter results into Telegram-ready HTML messages."""

    def format_signal(self, signal_result: dict) -> str:
        """
        Full signal message for approved signals.
        signal_result: the dict returned by SignalFilter.check() with approved=True
        """
        sig         = signal_result["signal"]
        new_conf    = signal_result["new_confidence"]
        n_passed    = len(signal_result["filters_passed"])
        pair        = sig.get("pair", "—")
        direction   = sig.get("direction", "—")
        strategy    = sig.get("strategy", "—")
        expiry      = sig.get("expiry", "1m")
        session     = sig.get("session", "NEUTRAL")
        entry_price = sig.get("entry_price", 0)
        orig_conf   = sig.get("confidence", 0)

        # Direction emoji
        if direction == "BUY":
            dir_text = "🟢 <b>ВВЕРХ (CALL)</b>"
        else:
            dir_text = "🔴 <b>ВНИЗ (PUT)</b>"

        # Expiry label
        exp_label = "1 минута" if expiry == "1m" else "2 минуты"

        # Strategy human name
        strat_name = _STRATEGY_NAMES.get(strategy, strategy)

        # Confidence level
        if new_conf >= 70:
            conf_label = f"СИЛЬНЫЙ 🔥🔥🔥 ({new_conf:.0f}%)"
        elif new_conf >= 60:
            conf_label = f"СРЕДНИЙ 🔥🔥 ({new_conf:.0f}%)"
        else:
            conf_label = f"БАЗОВЫЙ 🔥 ({new_conf:.0f}%)"

        # Session label
        session_emoji = {"BULL": "🐂 Бычья", "BEAR": "🐻 Медвежья", "NEUTRAL": "⚖️ Нейтральная"}
        sess_label = session_emoji.get(session, session)

        # Time
        now = datetime.datetime.now().strftime("%H:%M:%S")

        # Price
        price_str = f"{entry_price:.5f}" if entry_price else "—"

        lines = [
            "⚡ <b>СИГНАЛ ПОДТВЕРЖДЁН</b>",
            "",
            f"  Пара:       <b>{pair}</b>",
            f"  Направление: {dir_text}",
            f"  Экспирация:  <b>{exp_label}</b>",
            f"  Цена входа:  <code>{price_str}</code>",
            "",
            f"  Стратегия:   <i>{strat_name}</i>",
            f"  Уверенность: <b>{conf_label}</b>",
            f"  Сессия:      {sess_label}",
            "",
            f"  ✅ Фильтров пройдено: {n_passed}/15",
            f"  🕐 Время: {now}",
        ]
        return "\n".join(lines)

    def format_rejected(self, signal_result: dict) -> str:
        """
        Short rejection message (for admin/debug log, not users).
        """
        sig       = signal_result["signal"]
        reason    = signal_result["reason"]
        failed    = signal_result["filters_failed"]
        pair      = sig.get("pair", "—")
        direction = sig.get("direction", "—")
        strategy  = sig.get("strategy", "—")
        failed_str = failed[0] if failed else "?"

        return (f"❌ <b>ОТКЛОНЁН:</b> {pair} {direction} | "
                f"<i>{_STRATEGY_NAMES.get(strategy, strategy)}</i> | "
                f"Фильтр: <code>{failed_str}</code> — {reason}")

    def format_result(
        self,
        pair: str,
        direction: str,
        result: str,
        pnl: Optional[float] = None,
    ) -> str:
        """One-line trade result."""
        emoji = {"WIN": "✅", "LOSS": "❌", "DRAW": "➖"}.get(result, "❓")
        pnl_str = f" ({pnl:+.1f}%)" if pnl is not None else ""
        return f"{emoji} <b>{pair}</b> {direction} → {result}{pnl_str}"

    def format_stats(
        self,
        stats: dict,
        wins: int = 0,
        losses: int = 0,
    ) -> str:
        """Session statistics block."""
        total = wins + losses
        wr = (wins / total * 100) if total > 0 else 0.0
        active     = stats.get("active_trades", 0)
        today      = stats.get("trades_today", 0)
        last_hour  = stats.get("trades_last_hour", 0)
        loss_str   = stats.get("global_loss_streak", 0)
        paused     = stats.get("loss_paused", False)

        pause_note = ""
        if paused:
            until = stats.get("loss_pause_until", 0)
            import time
            remaining = max(0, int(until - time.time()))
            pause_note = f"\n  ⏸️ Пауза: ещё {remaining}с"

        wr_emoji = "🟢" if wr >= 60 else ("🟡" if wr >= 50 else "🔴")

        lines = [
            "📊 <b>Статистика сессии</b>",
            "",
            f"  {wr_emoji} Винрейт:       <b>{wr:.1f}%</b> ({wins}В / {losses}П / {total} итого)",
            f"  ⚡ Активных:       {active}",
            f"  📅 Сегодня:        {today}",
            f"  🕐 За час:         {last_hour}",
            f"  🔴 Серия потерь:   {loss_str}",
        ]
        if pause_note:
            lines.append(pause_note)

        return "\n".join(lines)
