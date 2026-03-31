import os
import time
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))


def _require(name: str, retries: int = 5, delay: float = 3.0) -> str:
    for attempt in range(retries):
        val = os.environ.get(name)
        if val:
            return val
        if attempt < retries - 1:
            print(f"[config] {name} not found, retrying in {delay}s ({attempt+1}/{retries})...", flush=True)
            time.sleep(delay)
    raise RuntimeError(
        f"Required env var '{name}' is not set after {retries} attempts. "
        f"Check Railway Variables."
    )


TELEGRAM_BOT_TOKEN: str = (
    os.environ.get("SIGNAL_BOT_TOKEN")
    or os.environ.get("TELEGRAM_BOT_TOKEN")
    or _require("SIGNAL_BOT_TOKEN")
)

ADMIN_USER_ID: int = int(_require("ADMIN_USER_ID"))

PO_LOGIN: str = _require("PO_LOGIN")
PO_PASSWORD: str = _require("PO_PASSWORD")

PP_LOGIN: str = os.environ.get("PP_LOGIN", "")
PP_PASSWORD: str = os.environ.get("PP_PASSWORD", "")

AUTO_TRADE: bool = os.getenv("AUTO_TRADE", "false").lower() == "true"

HEADLESS: bool = os.getenv("HEADLESS", "true").lower() == "true"

DB_PATH: str = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "signal_bot.db"))

PO_BASE_URL: str = "https://pocketoption.com"
PO_TRADE_URL: str = f"{PO_BASE_URL}/en/cabinet/demo-quick-high-low/"
PO_LOGIN_URL: str = f"{PO_BASE_URL}/en/login"

SIGNAL_CONFIDENCE_THRESHOLD: int = int(os.getenv("SIGNAL_CONFIDENCE_THRESHOLD", "3"))

OTC_PAIRS: list[dict] = [
    # Confirmed 92% payout (from live PocketOption data)
    # EUR/USD removed: test-17 4W/11L (26.7%) — structural weakness on level_touch
    {"label": "EUR/JPY OTC",  "symbol": "#EURJPY_otc",  "payout": 92},
    # GBP/JPY removed: consistently 25-33% WR across tests 11-15 (10 tests)
    {"label": "AUD/USD OTC",  "symbol": "#AUDUSD_otc",  "payout": 92},
    {"label": "AUD/CAD OTC",  "symbol": "#AUDCAD_otc",  "payout": 92},
    {"label": "AUD/NZD OTC",  "symbol": "#AUDNZD_otc",  "payout": 92},
    {"label": "CAD/CHF OTC",  "symbol": "#CADCHF_otc",  "payout": 92},
    # Additional pairs confirmed on PocketOption (92%)
    {"label": "AED/CNY OTC",  "symbol": "#AEDCNY_otc",  "payout": 92},
    {"label": "BHD/CNY OTC",  "symbol": "#BHDCNY_otc",  "payout": 92},
    {"label": "AED/USD OTC",  "symbol": "#AEDUSD_otc",  "payout": 92},
    {"label": "KES/USD OTC",  "symbol": "#KESUSD_otc",  "payout": 92},
    {"label": "CAD/JPY OTC",  "symbol": "#CADJPY_otc",  "payout": 92},
    {"label": "EUR/CHF OTC",  "symbol": "#EURCHF_otc",  "payout": 92},
    {"label": "GBP/AUD OTC",  "symbol": "#GBPAUD_otc",  "payout": 92},
    # Other pairs (payout varies — shown when no live data available)
    {"label": "GBP/USD OTC",  "symbol": "#GBPUSD_otc",  "payout": 82},
    {"label": "USD/JPY OTC",  "symbol": "#USDJPY_otc",  "payout": 82},
    # USD/CAD removed: test-17 5W/10L (33.3%)
    # USD/CHF removed: test-17 0W/4L (0%)
    {"label": "NZD/USD OTC",  "symbol": "#NZDUSD_otc",  "payout": 82},
    {"label": "EUR/GBP OTC",  "symbol": "#EURGBP_otc",  "payout": 82},
    {"label": "AUD/JPY OTC",  "symbol": "#AUDJPY_otc",  "payout": 82},
    {"label": "AUD/CHF OTC",  "symbol": "#AUDCHF_otc",  "payout": 82},
    {"label": "NZD/JPY OTC",  "symbol": "#NZDJPY_otc",  "payout": 82},
    # CHF/JPY removed: test-17 2W/6L (25%)
    {"label": "EUR/TRY OTC",  "symbol": "#EURTRY_otc",  "payout": 82},
]

EXPIRATIONS: list[dict] = [
    {"label": "1 мин", "seconds": 60},
    {"label": "2 мин", "seconds": 120},
]
