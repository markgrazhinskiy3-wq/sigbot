import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
PP_LOGIN: str = os.environ["PP_LOGIN"]
PP_PASSWORD: str = os.environ["PP_PASSWORD"]

_raw_ids = os.getenv("ALLOWED_USER_IDS", "")
ALLOWED_USER_IDS: list[int] = [
    int(uid.strip()) for uid in _raw_ids.split(",") if uid.strip().isdigit()
]

HEADLESS: bool = os.getenv("HEADLESS", "true").lower() == "true"

CACHE_TTL: int = int(os.getenv("CACHE_TTL", "90"))

PP_BASE_URL: str = "https://pocketpartners.com"
PP_DASHBOARD_URL: str = f"{PP_BASE_URL}/ru/dashboard"
PP_LOGIN_URL: str = f"{PP_BASE_URL}/ru/login"
