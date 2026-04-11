"""
po_login.py — Login to PocketOption from VPS and save session
=============================================================
Run this ONCE on your Aeza VPS to get a session bound to the VPS IP.
The session is saved to po_session.json and used by po_candle_server.py.

Setup:
  pip3 install playwright
  playwright install chromium

Usage:
  PO_LOGIN="your@email.com" PO_PASSWORD="yourpassword" python3 po_login.py
"""

import asyncio
import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("po-login")

PO_LOGIN    = os.environ.get("PO_LOGIN", "").strip()
PO_PASSWORD = os.environ.get("PO_PASSWORD", "").strip()
SESSION_FILE = os.path.join(os.path.dirname(__file__), "po_session.json")


async def login_and_capture() -> dict | None:
    from playwright.async_api import async_playwright

    captured_auth: dict | None = None
    captured_cookie: str = ""

    async with async_playwright() as pw:
        logger.info("Launching browser...")
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
        )
        page = await context.new_page()

        # Intercept WebSocket frames to capture auth payload
        ws_auth_event = asyncio.Event()

        def on_websocket(ws):
            async def on_frame_sent(payload: str):
                nonlocal captured_auth
                if captured_auth:
                    return
                try:
                    if payload.startswith("42"):
                        data = json.loads(payload[2:])
                        if isinstance(data, list) and data[0] == "auth":
                            auth = data[1]
                            if isinstance(auth, dict) and ("session" in auth or "uid" in auth):
                                captured_auth = auth
                                logger.info("WS auth captured: uid=%s", auth.get("uid", "?"))
                                ws_auth_event.set()
                except Exception:
                    pass

            ws.on("framesent", lambda payload: asyncio.create_task(on_frame_sent(payload)))

        page.on("websocket", on_websocket)

        logger.info("Opening PocketOption login page...")
        try:
            await page.goto("https://pocketoption.com/en/login", timeout=30000)
        except Exception as e:
            logger.error("Failed to open login page: %s", e)
            await browser.close()
            return None

        logger.info("Filling login form...")
        try:
            await page.wait_for_selector('input[name="email"], input[type="email"]', timeout=10000)
            await page.fill('input[name="email"], input[type="email"]', PO_LOGIN)
            await page.fill('input[name="password"], input[type="password"]', PO_PASSWORD)
            await page.click('button[type="submit"], .btn-login, input[type="submit"]')
        except Exception as e:
            logger.error("Could not fill login form: %s", e)
            await page.screenshot(path="/root/po_login_error.png")
            logger.info("Screenshot saved to /root/po_login_error.png")
            await browser.close()
            return None

        logger.info("Waiting for login and WebSocket auth capture...")
        try:
            await asyncio.wait_for(ws_auth_event.wait(), timeout=45.0)
        except asyncio.TimeoutError:
            logger.warning("WS auth not captured in time — trying to extract cookie instead")

        # Extract ci_session cookie as fallback
        cookies = await context.cookies()
        for cookie in cookies:
            if cookie["name"] == "ci_session":
                captured_cookie = cookie["value"]
                logger.info("ci_session cookie captured (len=%d)", len(captured_cookie))
                break

        await browser.close()

    if captured_auth:
        return captured_auth
    elif captured_cookie:
        logger.info("Using cookie-based auth")
        return {"session": captured_cookie, "isDemo": 1}
    else:
        logger.error("Could not capture auth — login may have failed")
        return None


async def main() -> None:
    if not PO_LOGIN or not PO_PASSWORD:
        logger.error("Set PO_LOGIN and PO_PASSWORD env vars")
        logger.error("Example: PO_LOGIN='you@email.com' PO_PASSWORD='pass' python3 po_login.py")
        sys.exit(1)

    logger.info("Logging into PocketOption with %s ...", PO_LOGIN)
    auth = await login_and_capture()

    if not auth:
        logger.error("Login failed. Check credentials or see /root/po_login_error.png")
        sys.exit(1)

    with open(SESSION_FILE, "w") as f:
        json.dump(auth, f, indent=2)

    logger.info("Session saved to %s", SESSION_FILE)
    logger.info("uid=%s, has_session=%s", auth.get("uid", "?"), "session" in auth)
    logger.info("")
    logger.info("Now run: python3 po_candle_server.py")


if __name__ == "__main__":
    asyncio.run(main())
