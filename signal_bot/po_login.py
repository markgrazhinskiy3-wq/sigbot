"""
po_login.py — Login to PocketOption from VPS and save session
=============================================================
Run this ONCE on your Aeza VPS.

Setup:
  pip3 install playwright aiohttp --break-system-packages
  playwright install chromium --with-deps

Usage:
  PO_LOGIN="your@email.com" PO_PASSWORD="yourpassword" python3 /root/po_login.py
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

PO_LOGIN     = os.environ.get("PO_LOGIN", "").strip()
PO_PASSWORD  = os.environ.get("PO_PASSWORD", "").strip()
SESSION_FILE = "/root/po_session.json"
SCREENSHOT   = "/root/po_login_debug.png"


async def try_fill(page, selectors: list[str], value: str) -> bool:
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                await el.fill(value)
                return True
        except Exception:
            pass
    return False


async def try_click(page, selectors: list[str]) -> bool:
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                await el.click()
                return True
        except Exception:
            pass
    return False


async def login_and_capture() -> dict | None:
    from playwright.async_api import async_playwright

    captured_auth: dict | None = None
    ws_auth_event = asyncio.Event()

    async with async_playwright() as pw:
        logger.info("Launching browser...")
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                  "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )

        # Mask webdriver flag
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """)

        page = await context.new_page()

        def on_websocket(ws):
            async def on_frame(payload):
                nonlocal captured_auth
                if captured_auth:
                    return
                try:
                    if isinstance(payload, str) and payload.startswith("42"):
                        data = json.loads(payload[2:])
                        if isinstance(data, list) and data[0] == "auth":
                            auth = data[1]
                            if isinstance(auth, dict) and ("session" in auth or "uid" in auth):
                                captured_auth = auth
                                logger.info("WS auth captured! uid=%s", auth.get("uid", "?"))
                                ws_auth_event.set()
                except Exception:
                    pass
            ws.on("framesent", lambda p: asyncio.create_task(on_frame(p)))

        page.on("websocket", on_websocket)

        logger.info("Opening login page...")
        try:
            await page.goto("https://pocketoption.com/en/login/", timeout=30000,
                            wait_until="domcontentloaded")
        except Exception as e:
            logger.error("Cannot open PocketOption: %s", e)
            await browser.close()
            return None

        await asyncio.sleep(2)
        await page.screenshot(path=SCREENSHOT)
        logger.info("Screenshot saved: %s", SCREENSHOT)

        logger.info("Filling email...")
        email_selectors = [
            'input[name="email"]',
            'input[type="email"]',
            '#email',
            '.email-input input',
        ]
        ok = await try_fill(page, email_selectors, PO_LOGIN)
        if not ok:
            logger.error("Email field not found — check screenshot: %s", SCREENSHOT)
            await page.screenshot(path=SCREENSHOT)
            await browser.close()
            return None

        logger.info("Filling password...")
        pass_selectors = [
            'input[name="password"]',
            'input[type="password"]',
            '#password',
        ]
        ok = await try_fill(page, pass_selectors, PO_PASSWORD)
        if not ok:
            logger.error("Password field not found")
            await browser.close()
            return None

        await asyncio.sleep(1)
        await page.screenshot(path=SCREENSHOT)

        logger.info("Clicking submit...")
        btn_selectors = [
            'button[type="submit"]',
            'input[type="submit"]',
            'button.btn-primary',
            'button.login-btn',
            '.login-form button',
            'form button',
        ]
        ok = await try_click(page, btn_selectors)
        if not ok:
            logger.warning("Submit button not found by selector — trying Enter key")
            await page.keyboard.press("Enter")

        logger.info("Waiting for login to complete...")
        await asyncio.sleep(3)
        await page.screenshot(path=SCREENSHOT)

        # Wait for redirect to cabinet
        try:
            await page.wait_for_url("**/cabinet/**", timeout=20000)
            logger.info("Redirected to cabinet — login successful")
        except Exception:
            logger.warning("No redirect to cabinet — may be on 2FA or captcha page")
            await page.screenshot(path=SCREENSHOT)

        # Wait for WS auth
        logger.info("Waiting for WebSocket auth capture (up to 30s)...")
        try:
            await asyncio.wait_for(ws_auth_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            logger.warning("WS auth not captured — extracting cookie as fallback")

        # Try cookie
        cookies = await context.cookies()
        ci_session = next((c["value"] for c in cookies if c["name"] == "ci_session"), "")

        await browser.close()

    if captured_auth:
        logger.info("Using WS auth payload")
        return captured_auth

    if ci_session:
        logger.info("Using ci_session cookie (len=%d)", len(ci_session))
        return {"session": ci_session, "isDemo": 1}

    logger.error("Login failed — no auth captured. Check screenshot: %s", SCREENSHOT)
    return None


async def main() -> None:
    if not PO_LOGIN or not PO_PASSWORD:
        logger.error("Set env vars: PO_LOGIN and PO_PASSWORD")
        sys.exit(1)

    logger.info("Logging into PocketOption as %s", PO_LOGIN)
    auth = await login_and_capture()

    if not auth:
        logger.error("Login failed. Check: %s", SCREENSHOT)
        sys.exit(1)

    with open(SESSION_FILE, "w") as f:
        json.dump(auth, f, indent=2)

    logger.info("Session saved → %s", SESSION_FILE)
    logger.info("uid=%s", auth.get("uid", "?"))
    logger.info("")
    logger.info("Next step: python3 /root/po_candle_server.py")


if __name__ == "__main__":
    asyncio.run(main())
