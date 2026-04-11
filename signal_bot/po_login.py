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
SCREENSHOT   = "/root/po_debug.png"


async def main() -> None:
    if not PO_LOGIN or not PO_PASSWORD:
        logger.error("Set PO_LOGIN and PO_PASSWORD env vars")
        sys.exit(1)

    from playwright.async_api import async_playwright

    logger.info("Logging in as %s ...", PO_LOGIN)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )
        await context.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        page = await context.new_page()

        # Navigate — ignore timeout, page may partially load due to blocked CDN resources
        logger.info("Opening login page (ignoring timeout)...")
        try:
            await page.goto(
                "https://pocketoption.com/en/login",
                timeout=15000,
                wait_until="commit",   # just wait for first server response
            )
        except Exception as e:
            logger.info("goto raised (expected if CDN slow): %s", type(e).__name__)

        # Wait for DOM to settle
        await asyncio.sleep(5)
        await page.screenshot(path=SCREENSHOT)
        logger.info("Screenshot saved: %s", SCREENSHOT)

        # Find email field
        email_input = None
        for sel in ['input[name="email"]', 'input[type="email"]', '#email']:
            try:
                el = await page.wait_for_selector(sel, timeout=8000)
                if el:
                    email_input = el
                    logger.info("Found email field: %s", sel)
                    break
            except Exception:
                pass

        if not email_input:
            logger.error("Login form not found — PO may be blocking this IP for browser access")
            logger.error("Check screenshot: %s", SCREENSHOT)
            await browser.close()
            sys.exit(1)

        # Fill email and password
        await email_input.fill(PO_LOGIN)
        logger.info("Email filled")

        pw_input = None
        for sel in ['input[name="password"]', 'input[type="password"]', '#password']:
            try:
                el = await page.query_selector(sel)
                if el:
                    pw_input = el
                    break
            except Exception:
                pass

        if not pw_input:
            logger.error("Password field not found")
            await browser.close()
            sys.exit(1)

        await pw_input.fill(PO_PASSWORD)
        logger.info("Password filled")

        await asyncio.sleep(1)
        await page.screenshot(path=SCREENSHOT)
        logger.info("Screenshot before submit: %s", SCREENSHOT)

        # Submit
        submitted = False
        for sel in ['button[type="submit"]', 'input[type="submit"]', 'form button', '.btn-primary', 'button']:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    submitted = True
                    logger.info("Clicked: %s", sel)
                    break
            except Exception:
                pass
        if not submitted:
            await page.keyboard.press("Enter")
            logger.info("Pressed Enter")

        # Wait for redirect or cookies to appear
        logger.info("Waiting 20s for login to complete...")
        await asyncio.sleep(20)
        await page.screenshot(path=SCREENSHOT)
        logger.info("Post-login screenshot: %s", SCREENSHOT)
        logger.info("Current URL: %s", page.url)

        # Get ci_session cookie
        cookies = await context.cookies()
        ci_session = next((c["value"] for c in cookies if c["name"] == "ci_session"), "")

        await browser.close()

    if not ci_session:
        logger.error("ci_session cookie not found after login")
        logger.error("Screenshot saved to: %s — check it to see what happened", SCREENSHOT)
        sys.exit(1)

    logger.info("ci_session captured! length=%d", len(ci_session))
    auth = {"session": ci_session, "isDemo": 1}

    with open(SESSION_FILE, "w") as f:
        json.dump(auth, f, indent=2)

    logger.info("Session saved → %s", SESSION_FILE)
    logger.info("Done! Now run: python3 /root/po_candle_server.py")


if __name__ == "__main__":
    asyncio.run(main())
