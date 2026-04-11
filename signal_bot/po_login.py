"""
po_login.py — Login to PocketOption via HTTP (no browser needed)
================================================================
Run this ONCE on your Aeza VPS.

Setup:
  pip3 install aiohttp --break-system-packages

Usage:
  PO_LOGIN="your@email.com" PO_PASSWORD="yourpassword" python3 /root/po_login.py
"""

import asyncio
import json
import logging
import os
import sys

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("po-login")

PO_LOGIN     = os.environ.get("PO_LOGIN", "").strip()
PO_PASSWORD  = os.environ.get("PO_PASSWORD", "").strip()
SESSION_FILE = "/root/po_session.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://pocketoption.com/en/login",
    "Origin": "https://pocketoption.com",
}


async def main() -> None:
    if not PO_LOGIN or not PO_PASSWORD:
        logger.error("Set PO_LOGIN and PO_PASSWORD env vars")
        sys.exit(1)

    logger.info("Logging into PocketOption as %s (HTTP, no browser)...", PO_LOGIN)

    jar = aiohttp.CookieJar()
    async with aiohttp.ClientSession(cookie_jar=jar, headers=HEADERS) as session:

        # Step 1: GET login page to get initial cookies
        logger.info("Step 1: GET login page...")
        try:
            async with session.get(
                "https://pocketoption.com/en/login",
                timeout=aiohttp.ClientTimeout(total=20),
                allow_redirects=True,
            ) as resp:
                logger.info("Login page: status=%d", resp.status)
                html = await resp.text()
        except Exception as e:
            logger.error("Cannot reach pocketoption.com: %s", e)
            sys.exit(1)

        # Step 2: POST login form
        logger.info("Step 2: Submitting login form...")
        login_data = {
            "email": PO_LOGIN,
            "password": PO_PASSWORD,
            "submitLogin": "1",
        }
        try:
            async with session.post(
                "https://pocketoption.com/en/login",
                data=login_data,
                timeout=aiohttp.ClientTimeout(total=20),
                allow_redirects=True,
            ) as resp:
                final_url = str(resp.url)
                status = resp.status
                logger.info("POST response: status=%d url=%s", status, final_url)
        except Exception as e:
            logger.error("Login POST failed: %s", e)
            sys.exit(1)

        # Step 3: extract ci_session cookie
        ci_session = ""
        for cookie in jar:
            if cookie.key == "ci_session":
                ci_session = cookie.value
                break

        # Also try from session cookies directly
        if not ci_session:
            cookies = session.cookie_jar.filter_cookies("https://pocketoption.com")
            ci_session = cookies.get("ci_session", {}).value if "ci_session" in cookies else ""

        if not ci_session:
            if "cabinet" in final_url or "dashboard" in final_url:
                logger.error("Redirected to cabinet but ci_session not found — try again")
            elif "login" in final_url:
                logger.error("Still on login page — wrong credentials or captcha required")
                logger.error("Try: open https://pocketoption.com in any browser and login manually")
            else:
                logger.error("Unexpected redirect: %s", final_url)
            sys.exit(1)

    logger.info("ci_session captured! length=%d", len(ci_session))
    logger.info("Final URL: %s", final_url)

    auth = {"session": ci_session, "isDemo": 1}
    with open(SESSION_FILE, "w") as f:
        json.dump(auth, f, indent=2)

    logger.info("Session saved → %s", SESSION_FILE)
    logger.info("")
    logger.info("Done! Now run: python3 /root/po_candle_server.py")


if __name__ == "__main__":
    asyncio.run(main())
