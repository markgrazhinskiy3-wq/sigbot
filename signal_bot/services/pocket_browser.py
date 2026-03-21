import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

logger = logging.getLogger(__name__)

_playwright = None
_browser: Browser | None = None
_context: BrowserContext | None = None

SCREENSHOTS_DIR = Path(os.path.dirname(__file__)).parent / "screenshots"
SCREENSHOTS_DIR.mkdir(exist_ok=True)


async def _get_context() -> BrowserContext:
    global _playwright, _browser, _context
    if _browser is None or not _browser.is_connected():
        if _playwright is None:
            _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(
            headless=config.HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        _context = await _browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        await _context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.info("Browser launched")
    return _context


async def _login(page: Page) -> None:
    logger.info("Logging into Pocket Option")
    await page.goto(config.PO_LOGIN_URL, wait_until="networkidle", timeout=30_000)
    await page.wait_for_timeout(1500)

    email_selectors = [
        'input[name="email"]',
        'input[type="email"]',
        'input[placeholder*="Email" i]',
        'input[placeholder*="email" i]',
    ]
    password_selectors = [
        'input[name="password"]',
        'input[type="password"]',
    ]
    submit_selectors = [
        'button[type="submit"]',
        'button:has-text("Sign in")',
        'button:has-text("Log in")',
        'button:has-text("Login")',
    ]

    for sel in email_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.fill(config.PO_LOGIN)
                break
        except Exception:
            continue

    for sel in password_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.fill(config.PO_PASSWORD)
                break
        except Exception:
            continue

    for sel in submit_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                await btn.click()
                break
        except Exception:
            continue

    await page.wait_for_load_state("networkidle", timeout=30_000)
    await page.wait_for_timeout(2000)

    if "login" in page.url.lower() or "auth" in page.url.lower():
        raise RuntimeError("Login failed — check credentials")
    logger.info("Login successful, URL: %s", page.url)


async def _is_logged_in(page: Page) -> bool:
    try:
        await page.goto(config.PO_TRADE_URL, wait_until="networkidle", timeout=20_000)
        await page.wait_for_timeout(1000)
        url = page.url.lower()
        return "login" not in url and "auth" not in url
    except Exception:
        return False


async def get_candles(symbol: str, count: int = 60) -> list[dict]:
    """
    Navigate to trading page for the given symbol and collect OHLC candle data.
    Uses WebSocket interception as primary method, DOM/JS scanning as fallback.
    Returns empty list if no real market data can be extracted — caller must
    treat this as NO_SIGNAL, never substitute synthetic data.
    """
    context = await _get_context()
    page = await context.new_page()
    ws_messages: list[str] = []

    async def handle_ws(ws):
        async def on_msg(msg):
            ws_messages.append(msg)
        ws.on("framereceived", on_msg)

    page.on("websocket", handle_ws)

    try:
        logged_in = await _is_logged_in(page)
        if not logged_in:
            await _login(page)
            await page.goto(config.PO_TRADE_URL, wait_until="networkidle", timeout=30_000)
            await page.wait_for_timeout(2000)

        symbol_clean = symbol.lstrip("#")
        trade_url = f"{config.PO_BASE_URL}/en/cabinet/demo-quick-high-low/?asset={symbol_clean}"
        await page.goto(trade_url, wait_until="networkidle", timeout=30_000)
        await page.wait_for_timeout(5000)

        candles = _extract_candles_from_ws(ws_messages, count)

        if len(candles) < 22:
            logger.info("WS gave %d candles, trying DOM/JS", len(candles))
            dom_candles = await _extract_candles_from_dom(page, count)
            if len(dom_candles) > len(candles):
                candles = dom_candles

        if len(candles) < 22:
            logger.warning(
                "Only %d real candles collected for %s — not enough for analysis",
                len(candles), symbol,
            )

        logger.info("Returning %d candles for %s", len(candles), symbol)
        return candles

    except Exception as e:
        logger.exception("get_candles failed: %s", e)
        raise
    finally:
        await page.close()


def _extract_candles_from_ws(messages: list[str], count: int) -> list[dict]:
    candles = []
    for msg in messages:
        try:
            data = json.loads(msg)
        except Exception:
            start = msg.find("{")
            if start == -1:
                continue
            try:
                data = json.loads(msg[start:])
            except Exception:
                continue

        extracted = _parse_candle_message(data)
        candles.extend(extracted)

    seen: set[tuple] = set()
    unique: list[dict] = []
    for c in candles:
        key = (c.get("open"), c.get("close"), c.get("high"), c.get("low"))
        if key not in seen:
            seen.add(key)
            unique.append(c)

    return unique[-count:] if len(unique) > count else unique


def _parse_candle_message(data: Any) -> list[dict]:
    candles: list[dict] = []

    if isinstance(data, dict):
        for key in ("candles", "history", "data", "items", "bars"):
            if key in data and isinstance(data[key], list):
                for item in data[key]:
                    c = _try_parse_candle(item)
                    if c:
                        candles.append(c)
                if candles:
                    return candles
        c = _try_parse_candle(data)
        if c:
            candles.append(c)

    elif isinstance(data, list):
        for item in data:
            c = _try_parse_candle(item)
            if c:
                candles.append(c)

    return candles


def _try_parse_candle(item: Any) -> dict | None:
    if isinstance(item, list) and len(item) >= 5:
        try:
            return {
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
            }
        except Exception:
            return None

    if isinstance(item, dict):
        mapping = [
            ("open",  ["open", "o", "Open"]),
            ("high",  ["high", "h", "High"]),
            ("low",   ["low",  "l", "Low"]),
            ("close", ["close", "c", "Close"]),
        ]
        result: dict = {}
        for field, aliases in mapping:
            for alias in aliases:
                if alias in item:
                    try:
                        result[field] = float(item[alias])
                        break
                    except Exception:
                        pass
        if len(result) == 4:
            return result
    return None


async def _extract_candles_from_dom(page: Page, count: int) -> list[dict]:
    try:
        candles = await page.evaluate(
            """(count) => {
                const result = [];
                const candidates = ['chartData', 'candles', 'quotes', 'history', '__chartData'];
                for (const key of candidates) {
                    if (window[key] && Array.isArray(window[key])) {
                        for (const item of window[key].slice(-count)) {
                            if (item && typeof item === 'object') {
                                const o = item.open ?? item.o;
                                const h = item.high ?? item.h;
                                const l = item.low ?? item.l;
                                const c = item.close ?? item.c;
                                if (o && h && l && c) {
                                    result.push({
                                        open: Number(o), high: Number(h),
                                        low: Number(l), close: Number(c)
                                    });
                                }
                            }
                        }
                        if (result.length > 5) return result;
                    }
                }
                for (const key of Object.keys(window)) {
                    try {
                        const val = window[key];
                        if (Array.isArray(val) && val.length > 5) {
                            const sample = val[0];
                            if (sample && typeof sample === 'object' &&
                                ('open' in sample || 'o' in sample)) {
                                for (const item of val.slice(-count)) {
                                    const o = item.open ?? item.o;
                                    const h = item.high ?? item.h;
                                    const l = item.low ?? item.l;
                                    const c = item.close ?? item.c;
                                    if (o && h && l && c) {
                                        result.push({
                                            open: Number(o), high: Number(h),
                                            low: Number(l), close: Number(c)
                                        });
                                    }
                                }
                                if (result.length > 5) return result;
                            }
                        }
                    } catch(e) {}
                }
                return result;
            }""",
            count,
        )
        return candles or []
    except Exception as e:
        logger.warning("DOM extraction failed: %s", e)
        return []


async def take_screenshot(symbol: str) -> str:
    """Take a screenshot of the trading chart and return the file path."""
    context = await _get_context()
    page = await context.new_page()
    try:
        logged_in = await _is_logged_in(page)
        if not logged_in:
            await _login(page)

        symbol_clean = symbol.lstrip("#")
        trade_url = f"{config.PO_BASE_URL}/en/cabinet/demo-quick-high-low/?asset={symbol_clean}"
        await page.goto(trade_url, wait_until="networkidle", timeout=30_000)
        await page.wait_for_timeout(3000)

        fname = f"result_{symbol_clean}_{int(time.time())}.png"
        path = str(SCREENSHOTS_DIR / fname)
        await page.screenshot(path=path, full_page=False)
        logger.info("Screenshot saved: %s", path)
        return path

    except Exception as e:
        logger.exception("Screenshot failed: %s", e)
        raise
    finally:
        await page.close()


async def close_browser() -> None:
    global _browser, _context, _playwright
    if _context:
        await _context.close()
        _context = None
    if _browser:
        await _browser.close()
        _browser = None
    if _playwright:
        await _playwright.stop()
        _playwright = None
    logger.info("Signal bot browser closed")
