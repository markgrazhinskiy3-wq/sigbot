import html
import logging
from dataclasses import dataclass

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from services.candle_cache import get_candles_cached as get_candles, get_cached_symbols, refresh_pair_now
from services.strategy_engine import calculate_signal, SignalResult
from bot.i18n import t as _t

logger = logging.getLogger(__name__)


@dataclass
class SignalResponse:
    direction: str       # "BUY" | "SELL" | "NO_SIGNAL"
    confidence: int
    details: dict
    pair: str
    expiration_sec: int
    symbol: str = ""     # raw symbol e.g. "#AUDCAD_otc"


async def scan_all_signals(pairs_map: dict[str, str]) -> list[SignalResponse]:
    """
    Scan all cached pairs and return a list of BUY/SELL signals sorted by confidence desc.
    `pairs_map` is {symbol: label}. Returns empty list if no signals found.

    Uses get_cached (fresh cache only) — never triggers live browser fetches.
    If a pair's cache is expired or empty, it is skipped silently.
    """
    from services.candle_cache import get_cached
    symbols = get_cached_symbols()
    results: list[SignalResponse] = []
    for symbol in symbols:
        candles = get_cached(symbol)   # cache-only: returns None if expired/missing
        if not candles:
            continue
        label = pairs_map.get(symbol, symbol)
        result: SignalResult = await calculate_signal(candles, symbol=symbol)
        if result.direction not in ("BUY", "SELL"):
            continue
        results.append(SignalResponse(
            direction=result.direction,
            confidence=result.confidence,
            details=result.details,
            pair=label,
            expiration_sec=_expiry_seconds(result.details),
            symbol=symbol,
        ))
    results.sort(key=lambda r: r.confidence, reverse=True)
    return results


async def get_signal(symbol: str, pair_label: str, expiration_sec: int) -> SignalResponse:
    logger.info("Fetching fresh signal for %s (exp=%ds)", symbol, expiration_sec)
    candles = await refresh_pair_now(symbol)
    if not candles:
        candles = await get_candles(symbol, count=80)
    expiry_str = "2m" if expiration_sec >= 120 else "1m"
    result: SignalResult = await calculate_signal(candles, expiry=expiry_str, symbol=symbol)
    return SignalResponse(
        direction=result.direction,
        confidence=result.confidence,
        details=result.details,
        pair=pair_label,
        expiration_sec=_expiry_seconds(result.details) if expiration_sec == 0 else expiration_sec,
        symbol=symbol,
    )


def _expiry_seconds(details: dict) -> int:
    """Convert expiry_hint ('1m'/'2m') to seconds."""
    hint = details.get("expiry_hint", "1m")
    return 120 if hint == "2m" else 60


def _conf_bar(confidence: int, total: int = 5) -> str:
    filled = min(confidence, total)
    return "🟩" * filled + "⬜" * (total - filled)


def _conf_label(confidence: int, lang: str = "ru") -> str:
    if confidence >= 5:
        return _t("conf_strong", lang)
    if confidence >= 4:
        return _t("conf_good", lang)
    return _t("conf_moderate", lang)


def _mode_label(mode: str, lang: str = "ru") -> str:
    key = {
        "TRENDING_UP":   "mode_trending_up",
        "TRENDING_DOWN": "mode_trending_down",
        "RANGE":         "mode_range",
        "VOLATILE":      "mode_volatile",
        "SQUEEZE":       "mode_squeeze",
    }.get(mode)
    return _t(key, lang) if key else ""


def _format_nosignal_debug(d: dict) -> list[str]:
    """Build human-readable rejection reason lines from engine debug dict."""
    import html as _html
    lines: list[str] = []
    dbg = d.get("debug", {})
    reject = d.get("reject_reason", "") or dbg.get("reason", "")

    # ── Case 1: score below per-strategy threshold ─────────────────────────
    if "score_below_threshold" in reject:
        pat   = dbg.get("best_pattern", "?")
        score = dbg.get("best_score", "?")
        thr   = dbg.get("min_score", "?")
        lines.append(f"• Паттерн: <b>{pat}</b>  score={score}")
        lines.append(f"• Порог для {pat}: <b>{thr}</b>")
        lines.append(f"• ❌ ОТКЛОНЁН: score ниже порога")
        return lines

    # ── Case 2: no patterns passed filters ────────────────────────────────
    if "no_patterns_passed" in reject:
        candidates = dbg.get("candidates_checked", [])
        filter_log = dbg.get("filter_rejections", dbg.get("filter_log", []))
        if not candidates:
            lines.append("• Паттернов не обнаружено вообще")
        else:
            lines.append(f"• Обнаружено паттернов: {len(candidates)}")
            for c in candidates[:3]:
                lines.append(f"  – {c.get('name','?')} {c.get('direction','?')} score={c.get('final_score', c.get('score','?'))}")
        if filter_log:
            lines.append("• Причины фильтрации:")
            for r in filter_log[:5]:
                short = _html.escape(str(r)[:120])
                lines.append(f"  – {short}")
        return lines

    # ── Case 3: validation / not enough candles ───────────────────────────
    if "too_few_candles" in reject or "validation" in reject:
        n = dbg.get("n") or d.get("candles_raw") or d.get("candles_clean")
        lines.append(f"• ❌ Недостаточно свечей: {n}")
        return lines

    # ── Fallback ──────────────────────────────────────────────────────────
    lines.append(f"• Причина: {_html.escape(str(reject)[:200])}")
    filter_log = dbg.get("filter_log", [])
    for r in filter_log[:3]:
        lines.append(f"  – {_html.escape(str(r)[:120])}")
    return lines


def format_signal_message(signal: SignalResponse, *, is_admin: bool = False, lang: str = "ru") -> str:
    if signal.direction == "NO_SIGNAL":
        d        = signal.details if isinstance(signal.details, dict) else {}
        mode     = d.get("market_mode", "")
        mode_lbl = _mode_label(mode, lang) or d.get("regime_label", "")

        header = _t("no_signal_header", lang)
        if mode_lbl:
            header += f" — {mode_lbl}"

        lines = [
            f"🔍 <b>{html.escape(signal.pair)}</b>",
            "",
            header,
            "",
        ]

        if is_admin:
            debug_lines = _format_nosignal_debug(d)
            lines.append(_t("no_signal_reason", lang))
            lines.extend(debug_lines)
        else:
            lines.append(_t("no_signal_ambiguous", lang))

        lines.append("")
        lines.append(_t("no_signal_action", lang))
        return "\n".join(lines)

    if signal.direction == "BUY":
        arrow     = "⬆️"
        dir_label = _t("signal_dir_buy", lang)
    else:
        arrow     = "⬇️"
        dir_label = _t("signal_dir_sell", lang)

    bar   = _conf_bar(signal.confidence)
    label = _conf_label(signal.confidence, lang)

    d = signal.details if isinstance(signal.details, dict) else {}
    explanation_lines = _build_explanation(signal.direction, d, lang)

    lines = [
        f"📊 <b>{html.escape(signal.pair)}</b>",
        "",
        _t("signal_label", lang, arrow=arrow, dir=dir_label),
        _t("signal_confidence", lang, bar=bar, conf=signal.confidence, label=label),
        "",
    ]

    if explanation_lines:
        lines.append(_t("signal_why_header", lang))
        for item in explanation_lines:
            lines.append(f"• {item}")
        lines.append("")

    lines.append(_t("signal_open_trade", lang))
    return "\n".join(lines)


def _build_explanation(direction: str, details: dict, lang: str = "ru") -> list[str]:
    """Build 2-4 bullet points explaining the signal in the user's language."""
    is_buy   = direction == "BUY"
    strategy = details.get("primary_strategy") or ""
    mode     = details.get("market_mode", "")
    quality  = details.get("signal_quality", "")
    debug    = details.get("debug", {})

    items: list[str] = []

    # 1. Strategy-specific main reason
    if strategy == "ema_bounce":
        items.append(_t("exp_ema_bounce_buy" if is_buy else "exp_ema_bounce_sell", lang))
    elif strategy in ("level_rejection", "level_touch"):
        items.append(_t("exp_level_rejection_buy" if is_buy else "exp_level_rejection_sell", lang))
    elif strategy == "false_breakout":
        items.append(_t("exp_false_breakout_buy" if is_buy else "exp_false_breakout_sell", lang))
    elif strategy in ("compression_breakout", "level_breakout"):
        items.append(_t("exp_compression_buy" if is_buy else "exp_compression_sell", lang))
    elif strategy == "impulse_pullback":
        items.append(_t("exp_impulse_pullback_buy" if is_buy else "exp_impulse_pullback_sell", lang))
    else:
        items.append(_t("exp_default_buy" if is_buy else "exp_default_sell", lang))

    # 2. Market mode context
    if mode == "TRENDING_UP" and is_buy:
        items.append(_t("exp_mode_trending_up_buy", lang))
    elif mode == "TRENDING_DOWN" and not is_buy:
        items.append(_t("exp_mode_trending_down_sell", lang))
    elif mode == "RANGE":
        items.append(_t("exp_mode_range_buy" if is_buy else "exp_mode_range_sell", lang))
    elif mode == "VOLATILE":
        items.append(_t("exp_mode_volatile", lang))
    elif mode == "SQUEEZE":
        items.append(_t("exp_mode_squeeze", lang))

    # 3. Indicator confirmation
    ind = debug.get("indicators", {})
    rsi_val = ind.get("rsi", 50)
    if is_buy and rsi_val < 35:
        items.append(_t("exp_ind_buy", lang))
    elif not is_buy and rsi_val > 65:
        items.append(_t("exp_ind_sell", lang))

    # 4. Quality note
    if quality in ("strong", "good"):
        items.append(_t("exp_quality_strong", lang))

    return items[:4]


def format_result_caption(
    pair_label: str,
    direction: str,
    expiration_sec: int,
    details: dict,
    outcome: str = "unknown",
) -> str:
    is_buy = direction == "BUY"
    header = (
        f"📈 <b>{html.escape(pair_label)} — ВВЕРХ</b>"
        if is_buy else
        f"📉 <b>{html.escape(pair_label)} — ВНИЗ</b>"
    )

    strategy = details.get("primary_strategy", "")
    mode     = details.get("market_mode", "")
    quality  = details.get("signal_quality", "")
    mode_lbl = _mode_label(mode)
    debug    = details.get("debug", {})
    ind      = debug.get("indicators", {})

    reasons = []

    rsi_val = ind.get("rsi", 50)
    if is_buy and rsi_val < 40:
        reasons.append(f"📊 Рынок слишком сильно упал и готов к отскоку вверх (RSI {rsi_val:.0f})")
    elif not is_buy and rsi_val > 60:
        reasons.append(f"📊 Рынок слишком сильно вырос и готов к откату вниз (RSI {rsi_val:.0f})")

    if strategy == "ema_bounce":
        if is_buy:
            reasons.append("📈 Отскок от скользящей средней — тренд продолжается вверх")
        else:
            reasons.append("📉 Отскок от скользящей средней — тренд продолжается вниз")

    elif strategy == "level_breakout":
        if is_buy:
            reasons.append("📈 Пробой уровня сопротивления вверх")
        else:
            reasons.append("📉 Пробой уровня поддержки вниз")

    reasons_text = "\n".join(reasons) if reasons else "• Большинство факторов сошлись в одном направлении"

    if outcome == "win":
        outcome_line = "✅ <b>Сделка закрыта в плюс!</b>"
    elif outcome == "loss":
        outcome_line = "❌ <b>Сделка закрыта в минус</b>"
    else:
        outcome_line = "📋 Результат — смотри скриншот выше"

    lines = [
        header,
        f"<i>Время сделки: {expiration_sec} сек</i>",
        "",
        outcome_line,
        "",
        "<b>Почему был дан именно этот сигнал:</b>",
        reasons_text,
    ]
    return "\n".join(lines)
