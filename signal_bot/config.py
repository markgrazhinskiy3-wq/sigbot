import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

TELEGRAM_BOT_TOKEN: str = os.environ.get("SIGNAL_BOT_TOKEN") or os.environ["TELEGRAM_BOT_TOKEN"]

ADMIN_USER_ID: int = int(os.environ["ADMIN_USER_ID"])

PO_LOGIN: str = os.environ["PO_LOGIN"]
PO_PASSWORD: str = os.environ["PO_PASSWORD"]

AUTO_TRADE: bool = os.getenv("AUTO_TRADE", "false").lower() == "true"

HEADLESS: bool = os.getenv("HEADLESS", "true").lower() == "true"

DB_PATH: str = os.path.join(os.path.dirname(__file__), "signal_bot.db")

PO_BASE_URL: str = "https://pocketoption.com"
PO_TRADE_URL: str = f"{PO_BASE_URL}/en/cabinet/demo-quick-high-low/"
PO_LOGIN_URL: str = f"{PO_BASE_URL}/en/login"

SIGNAL_CONFIDENCE_THRESHOLD: int = int(os.getenv("SIGNAL_CONFIDENCE_THRESHOLD", "3"))

OTC_PAIRS: list[dict] = [
    # Major pairs
    {"label": "EUR/USD OTC", "symbol": "#EURUSD_otc"},
    {"label": "GBP/USD OTC", "symbol": "#GBPUSD_otc"},
    {"label": "USD/JPY OTC", "symbol": "#USDJPY_otc"},
    {"label": "AUD/USD OTC", "symbol": "#AUDUSD_otc"},
    {"label": "USD/CAD OTC", "symbol": "#USDCAD_otc"},
    {"label": "USD/CHF OTC", "symbol": "#USDCHF_otc"},
    {"label": "NZD/USD OTC", "symbol": "#NZDUSD_otc"},
    # Cross pairs
    {"label": "EUR/GBP OTC", "symbol": "#EURGBP_otc"},
    {"label": "EUR/JPY OTC", "symbol": "#EURJPY_otc"},
    {"label": "GBP/JPY OTC", "symbol": "#GBPJPY_otc"},
    {"label": "AUD/JPY OTC", "symbol": "#AUDJPY_otc"},
    {"label": "AUD/CAD OTC", "symbol": "#AUDCAD_otc"},
    {"label": "AUD/CHF OTC", "symbol": "#AUDCHF_otc"},
    {"label": "AUD/NZD OTC", "symbol": "#AUDNZD_otc"},
    {"label": "CAD/CHF OTC", "symbol": "#CADCHF_otc"},
    {"label": "NZD/JPY OTC", "symbol": "#NZDJPY_otc"},
    {"label": "CHF/JPY OTC", "symbol": "#CHFJPY_otc"},
    {"label": "EUR/TRY OTC", "symbol": "#EURTRY_otc"},
]

EXPIRATIONS: list[dict] = [
    {"label": "1 мин", "seconds": 60},
    {"label": "2 мин", "seconds": 120},
]
