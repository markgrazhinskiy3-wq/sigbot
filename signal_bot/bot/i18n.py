"""
i18n module: all user-facing strings in Russian and English.
Usage:  from bot.i18n import t, get_lang, set_lang, _user_lang
"""

from __future__ import annotations

_user_lang: dict[int, str] = {}
_terms_accepted: set[int] = set()


def has_accepted_terms(user_id: int) -> bool:
    return user_id in _terms_accepted


def accept_terms(user_id: int) -> None:
    _terms_accepted.add(user_id)


def get_lang(user_id: int) -> str:
    return _user_lang.get(user_id, "ru")


def set_lang(user_id: int, lang: str) -> None:
    _user_lang[user_id] = lang


def load_langs_from_db(langs: dict[int, str]) -> None:
    """Populate in-memory cache from DB on startup. Call once after DB init."""
    _user_lang.update(langs)


def t(key: str, lang: str = "ru", **kwargs) -> str:
    s = _STRINGS.get(lang, _STRINGS["ru"]).get(key)
    if s is None:
        s = _STRINGS["ru"].get(key, key)
    if kwargs:
        try:
            return s.format(**kwargs)
        except (KeyError, IndexError):
            return s
    return s


_STRINGS: dict[str, dict[str, str]] = {
    "ru": {
        # ── Language selection ─────────────────────────────────────────────────
        "select_lang": "🌐 Выберите язык:",
        "lang_btn_ru": "🇷🇺 Русский",
        "lang_btn_en": "🇬🇧 English",
        "lang_set":    "✅ Язык установлен: Русский",

        # ── Start / status ─────────────────────────────────────────────────────
        "welcome":        "👋 <b>Pocket Option Signal Bot</b>\n\nВыберите действие:",
        "pending_msg":    "⏳ <b>Ожидайте одобрения администратора.</b>\n\nВаша заявка на доступ отправлена. Вы получите уведомление после рассмотрения.",
        "denied_msg":     "⛔ Доступ к боту запрещён.",
        "access_pending": "⏳ Ваша заявка ещё на рассмотрении.",
        "access_denied":  "⛔ Доступ запрещён.",
        "access_none":    "⛔ Нет доступа. Напишите /start",
        "access_granted": "✅ <b>Доступ одобрен!</b>\n\nДобро пожаловать в Signal Bot. Нажмите /start",
        "access_revoked": "⛔ Ваш запрос на доступ отклонён.",

        # ── Main menu ──────────────────────────────────────────────────────────
        "main_menu_title":    "👋 <b>Pocket Option Signal Bot</b>\n\nВыберите действие:",
        "btn_recommended":    "📊 Рекомендуемые пары",
        "btn_restart":        "🔁 Перезапустить бота",
        "btn_auto_on":        "🔔 Авто-сигналы: ВКЛ",
        "btn_auto_off":       "🔕 Авто-сигналы: ВЫКЛ",
        "btn_change_lang":    "🌐 Язык / Language",
        "btn_back":           "⬅️ Назад",
        "btn_main_menu":      "🏠 Главное меню",
        "btn_refresh":        "🔄 Обновить",
        "btn_refresh_list":   "🔄 Обновить список",
        "btn_retry":          "🔄 Попробовать снова",
        "btn_monitor_on":     "🔔 Включить мониторинг",
        "btn_monitor_off":    "⏹ Остановить мониторинг",
        "btn_next_signal":    "🔄 Следующий сигнал",
        "btn_other_pair":     "🔀 Выбрать другую пару",

        # ── Monitoring ─────────────────────────────────────────────────────────
        "monitor_start":    "🔔 <b>Мониторинг запущен — {pair}</b>\n\nБот отслеживает пару в реальном времени (до 5 минут).\nКак только появится сигнал — пришлёт уведомление автоматически.\n\n<i>Нажмите «Остановить», чтобы отменить мониторинг.</i>",
        "monitor_start_cb": "🔔 Мониторинг запущен",
        "monitor_stop":     "⏹ <b>Мониторинг остановлен — {pair}</b>\n\nВы можете запросить сигнал вручную или выбрать другую пару.",
        "monitor_stop_cb":  "Мониторинг остановлен",
        "monitor_timeout":  "⏱ <b>Мониторинг завершён — {pair}</b>\n\nЗа 5 минут сигнал не появился.\nПопробуйте другую пару или запустите мониторинг снова.",
        "monitor_worsened": "⚠️ <b>Условия на {pair} ухудшились</b>\n\nСкор торгуемости упал до {score}/100.\nРекомендуем выбрать другую пару.",

        # ── Auto-signals ───────────────────────────────────────────────────────
        "auto_enabled":       "🔔 <b>Авто-сигналы включены</b>\n\nБот будет присылать сигналы автоматически, когда видит хорошую точку входа.\n⏱ Экспирация: <b>2 минуты</b>",
        "auto_disabled":      "🔕 <b>Авто-сигналы отключены</b>\n\nВы больше не будете получать автоматические сигналы.",
        "auto_signal_header": "🤖 <b>Авто-сигнал</b> (2 мин)",
        "auto_pre_alert":     "📌 <b>Готовьтесь!</b>\n\nОткрывайте в Pocket Option:\n<b>{pair}</b>\n\n⏳ Бот ждёт подтверждения на новых свечах — сигнал придёт через несколько секунд.",
        "auto_signal_cancelled": "❌ <b>Сигнал отменён — {pair}</b>\n\nУсловия изменились после оповещения. Ждите следующего.",

        # ── Pair selection ─────────────────────────────────────────────────────
        "select_pair":     "Выберите пару для анализа:",
        "no_pairs":        "Нет доступных пар. Попробуйте позже.",

        # ── Risk disclaimer ────────────────────────────────────────────────────
        "risk_warning": (
            "⚠️ <b>Предупреждение о рисках</b>\n\n"
            "Торговля на финансовых рынках связана с риском.\n"
            "Сигналы, которые предоставляет бот, <b>не являются финансовой рекомендацией</b> "
            "и не гарантируют получение прибыли.\n\n"
            "Все решения о входе в сделку вы принимаете самостоятельно и несёте полную "
            "ответственность за результат торговли.\n\n"
            "Бот предоставляет торговые сигналы и аналитику, но <b>может ошибаться</b>.\n\n"
            "<b>Вы самостоятельно:</b>\n"
            "• принимаете решения о входе в сделки\n"
            "• выбираете сумму сделки\n"
            "• несёте ответственность за результат торговли\n\n"
            "<b>Мы настоятельно рекомендуем:</b>\n"
            "• соблюдать риск-менеджмент\n"
            "• не входить более чем на 1–2% от депозита\n"
            "• не торговать на последние деньги\n"
            "• начинать с минимальных сумм\n\n"
            "Нажимая кнопку ниже, вы подтверждаете, что понимаете риски "
            "и принимаете ответственность за свою торговлю."
        ),
        "btn_accept_terms": "✅ Принимаю условия",
    },

    "en": {
        # ── Language selection ─────────────────────────────────────────────────
        "select_lang": "🌐 Select language:",
        "lang_btn_ru": "🇷🇺 Русский",
        "lang_btn_en": "🇬🇧 English",
        "lang_set":    "✅ Language set: English",

        # ── Start / status ─────────────────────────────────────────────────────
        "welcome":        "👋 <b>Pocket Option Signal Bot</b>\n\nSelect an action:",
        "pending_msg":    "⏳ <b>Waiting for admin approval.</b>\n\nYour access request has been sent. You will be notified once it is reviewed.",
        "denied_msg":     "⛔ Access to this bot is denied.",
        "access_pending": "⏳ Your request is still under review.",
        "access_denied":  "⛔ Access denied.",
        "access_none":    "⛔ No access. Please type /start",
        "access_granted": "✅ <b>Access granted!</b>\n\nWelcome to Signal Bot. Press /start",
        "access_revoked": "⛔ Your access request has been denied.",

        # ── Main menu ──────────────────────────────────────────────────────────
        "main_menu_title":    "👋 <b>Pocket Option Signal Bot</b>\n\nSelect an action:",
        "btn_recommended":    "📊 Recommended pairs",
        "btn_restart":        "🔁 Restart bot",
        "btn_auto_on":        "🔔 Auto-signals: ON",
        "btn_auto_off":       "🔕 Auto-signals: OFF",
        "btn_change_lang":    "🌐 Язык / Language",
        "btn_back":           "⬅️ Back",
        "btn_main_menu":      "🏠 Main menu",
        "btn_refresh":        "🔄 Refresh",
        "btn_refresh_list":   "🔄 Refresh list",
        "btn_retry":          "🔄 Try again",
        "btn_monitor_on":     "🔔 Start monitoring",
        "btn_monitor_off":    "⏹ Stop monitoring",
        "btn_next_signal":    "🔄 Next signal",
        "btn_other_pair":     "🔀 Choose another pair",

        # ── Monitoring ─────────────────────────────────────────────────────────
        "monitor_start":    "🔔 <b>Monitoring started — {pair}</b>\n\nBot is watching the pair in real time (up to 5 minutes).\nYou will be notified automatically when a signal appears.\n\n<i>Press «Stop» to cancel monitoring.</i>",
        "monitor_start_cb": "🔔 Monitoring started",
        "monitor_stop":     "⏹ <b>Monitoring stopped — {pair}</b>\n\nYou can request a signal manually or choose another pair.",
        "monitor_stop_cb":  "Monitoring stopped",
        "monitor_timeout":  "⏱ <b>Monitoring finished — {pair}</b>\n\nNo signal appeared in 5 minutes.\nTry another pair or restart monitoring.",
        "monitor_worsened": "⚠️ <b>Conditions on {pair} deteriorated</b>\n\nTradability score dropped to {score}/100.\nWe recommend choosing another pair.",

        # ── Auto-signals ───────────────────────────────────────────────────────
        "auto_enabled":       "🔔 <b>Auto-signals enabled</b>\n\nThe bot will send signals automatically when it spots a good entry.\n⏱ Expiry: <b>2 minutes</b>",
        "auto_disabled":      "🔕 <b>Auto-signals disabled</b>\n\nYou will no longer receive automatic signals.",
        "auto_signal_header": "🤖 <b>Auto-signal</b> (2 min)",
        "auto_pre_alert":     "📌 <b>Get ready!</b>\n\nOpen in Pocket Option:\n<b>{pair}</b>\n\n⏳ Bot is waiting for confirmation on fresh candles — signal coming in a few seconds.",
        "auto_signal_cancelled": "❌ <b>Signal cancelled — {pair}</b>\n\nConditions changed after the alert. Wait for the next one.",

        # ── Pair selection ─────────────────────────────────────────────────────
        "select_pair":     "Select a pair to analyse:",
        "no_pairs":        "No pairs available. Please try later.",

        # ── Risk disclaimer ────────────────────────────────────────────────────
        "risk_warning": (
            "⚠️ <b>Risk Warning</b>\n\n"
            "Trading financial markets involves risk.\n"
            "Signals provided by this bot <b>are not financial advice</b> "
            "and do not guarantee profit.\n\n"
            "All trading decisions are made solely by you, and you bear full "
            "responsibility for the outcome of your trades.\n\n"
            "The bot provides trading signals and analytics but <b>can be wrong</b>.\n\n"
            "<b>You independently:</b>\n"
            "• make decisions to enter trades\n"
            "• choose the trade amount\n"
            "• bear responsibility for the trading outcome\n\n"
            "<b>We strongly recommend:</b>\n"
            "• practise proper risk management\n"
            "• risk no more than 1–2% of your deposit per trade\n"
            "• never trade money you cannot afford to lose\n"
            "• start with minimum amounts\n\n"
            "By pressing the button below you confirm that you understand the risks "
            "and accept responsibility for your own trading."
        ),
        "btn_accept_terms": "✅ I accept the terms",
    },
}
