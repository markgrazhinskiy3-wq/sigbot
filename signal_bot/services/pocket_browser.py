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

COOKIES_PATH = Path(os.path.dirname(__file__)).parent / "po_cookies.json"


def _load_saved_cookies() -> list[dict] | None:
    """Load cookies previously exported from browser via set_cookies.py."""
    if not COOKIES_PATH.exists():
        return None
    try:
        with open(COOKIES_PATH) as f:
            cookies = json.load(f)
        if isinstance(cookies, list) and len(cookies) > 0:
            logger.info("Loaded %d saved cookies from %s", len(cookies), COOKIES_PATH)
            return cookies
    except Exception as e:
        logger.warning("Failed to load saved cookies: %s", e)
    return None


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
    await page.goto(config.PO_LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(3000)

    logger.info("Login page URL: %s", page.url)

    email_selectors = [
        'input[name="email"]',
        'input[type="email"]',
        'input[placeholder*="Email" i]',
        'input[placeholder*="mail" i]',
        'input[autocomplete="email"]',
        'input[autocomplete="username"]',
        'form input[type="text"]',
        'input.email',
        '#email',
        '#login',
    ]
    password_selectors = [
        'input[name="password"]',
        'input[type="password"]',
        'input[placeholder*="Password" i]',
        'input[placeholder*="пароль" i]',
        '#password',
    ]
    submit_selectors = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Sign in")',
        'button:has-text("Log in")',
        'button:has-text("Login")',
        'button:has-text("Войти")',
        'button:has-text("Вход")',
        'form button',
        '.login-btn',
        '.btn-login',
        '.submit-btn',
    ]

    email_filled = False
    for sel in email_selectors:
        try:
            el = page.locator(sel).first
            cnt = await el.count()
            if cnt > 0:
                await el.wait_for(state="visible", timeout=3000)
                await el.click()
                await el.fill("")
                await el.type(config.PO_LOGIN, delay=50)
                email_filled = True
                logger.info("Email filled via selector: %s", sel)
                break
        except Exception:
            continue

    if not email_filled:
        debug_path = str(SCREENSHOTS_DIR / "login_debug_email.png")
        await page.screenshot(path=debug_path)
        logger.error("Could not find email field. Debug screenshot: %s", debug_path)
        raise RuntimeError("Login failed — email field not found on login page")

    await page.wait_for_timeout(500)

    password_filled = False
    for sel in password_selectors:
        try:
            el = page.locator(sel).first
            cnt = await el.count()
            if cnt > 0:
                await el.wait_for(state="visible", timeout=3000)
                await el.click()
                await el.fill("")
                await el.type(config.PO_PASSWORD, delay=50)
                password_filled = True
                logger.info("Password filled via selector: %s", sel)
                break
        except Exception:
            continue

    if not password_filled:
        debug_path = str(SCREENSHOTS_DIR / "login_debug_password.png")
        await page.screenshot(path=debug_path)
        logger.error("Could not find password field. Debug screenshot: %s", debug_path)
        raise RuntimeError("Login failed — password field not found on login page")

    await page.wait_for_timeout(500)

    clicked = False
    for sel in submit_selectors:
        try:
            btn = page.locator(sel).first
            cnt = await btn.count()
            if cnt > 0:
                await btn.wait_for(state="visible", timeout=3000)
                await btn.click()
                clicked = True
                logger.info("Submit clicked via selector: %s", sel)
                break
        except Exception:
            continue

    if not clicked:
        await page.keyboard.press("Enter")
        logger.info("Submit via Enter key")

    try:
        await page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass
    await page.wait_for_timeout(3000)

    current_url = page.url.lower()
    logger.info("Post-login URL: %s", current_url)

    success_indicators = ["cabinet", "trade", "dashboard", "platform"]
    fail_indicators = ["login", "signin", "auth", "register"]

    is_success = any(ind in current_url for ind in success_indicators)
    is_fail = any(ind in current_url for ind in fail_indicators)

    if is_fail and not is_success:
        debug_path = str(SCREENSHOTS_DIR / "login_debug_fail.png")
        await page.screenshot(path=debug_path)
        logger.error("Login failed, URL=%s. Debug screenshot: %s", current_url, debug_path)
        raise RuntimeError("Login failed — check credentials or captcha")

    logger.info("Login successful, URL: %s", page.url)


async def _try_cookie_login(context: BrowserContext, page: Page) -> bool:
    """Attempt login via saved cookies. Returns True if successful."""
    cookies = _load_saved_cookies()
    if not cookies:
        return False
    try:
        await context.add_cookies(cookies)
        await page.goto(config.PO_TRADE_URL, wait_until="domcontentloaded", timeout=20_000)
        await page.wait_for_timeout(2000)
        url = page.url.lower()
        success = "login" not in url and "auth" not in url
        if success:
            logger.info("Cookie login successful, URL: %s", page.url)
        else:
            logger.warning("Cookie login failed (URL: %s) — cookies may be expired", page.url)
        return success
    except Exception as e:
        logger.warning("Cookie login error: %s", e)
        return False


async def _is_logged_in(page: Page) -> bool:
    try:
        await page.goto(config.PO_TRADE_URL, wait_until="domcontentloaded", timeout=20_000)
        await page.wait_for_timeout(1000)
        url = page.url.lower()
        return "login" not in url and "auth" not in url
    except Exception:
        return False


async def _ensure_logged_in(context: BrowserContext, page: Page) -> None:
    """Ensure session is active: try cookies first, then automated login."""
    logged_in = await _is_logged_in(page)
    if logged_in:
        return
    cookie_ok = await _try_cookie_login(context, page)
    if not cookie_ok:
        await _login(page)


async def get_candles(symbol: str, count: int = 60) -> list[dict]:
    """
    Navigate to trading page for the given symbol and collect OHLC candle data.
    Uses WebSocket binary-frame interception (Socket.IO protocol).
    Pocket Option sends ticks as [[timestamp, price], ...] in binary frames.
    Ticks are aggregated into 60-second OHLC candles.
    Returns empty list if no real market data can be extracted — caller must
    treat this as NO_SIGNAL, never substitute synthetic data.
    """
    context = await _get_context()
    page = await context.new_page()

    # Socket.IO binary event tracking:
    # text frames  "451-["eventName",{placeholder}]" announce the event name
    # the next binary frame is the actual payload (JSON bytes)
    binary_frames: list[tuple[str, bytes]] = []
    last_event: list[str | None] = [None]

    def handle_ws(ws):
        if "po.market" not in ws.url:
            return
        def on_msg(msg):
            if isinstance(msg, str) and msg.startswith("451-"):
                try:
                    last_event[0] = json.loads(msg[4:])[0]
                except Exception:
                    pass
            elif isinstance(msg, bytes) and last_event[0]:
                binary_frames.append((last_event[0], msg))
        ws.on("framereceived", on_msg)

    page.on("websocket", handle_ws)

    try:
        await _ensure_logged_in(context, page)

        symbol_clean = symbol.lstrip("#")
        # Pocket Option internal assets use "#" prefix; encode it in the URL
        trade_url = f"{config.PO_BASE_URL}/en/cabinet/demo-quick-high-low/?asset=%23{symbol_clean}"
        await page.goto(trade_url, wait_until="domcontentloaded", timeout=30_000)

        # Wait for history data to arrive (up to 20s)
        deadline = 20
        for _ in range(deadline * 2):
            await page.wait_for_timeout(500)
            if any(ev == "updateHistoryNewFast" for ev, _ in binary_frames):
                break

        candles = _candles_from_binary_frames(binary_frames, count, period=30)
        logger.info("Binary frames gave %d candles for %s", len(candles), symbol)

        if len(candles) < 14:
            logger.warning("Only %d candles for %s — insufficient for analysis", len(candles), symbol)

        return candles

    except Exception as e:
        logger.exception("get_candles failed: %s", e)
        raise
    finally:
        await page.close()


def _ticks_to_candles(ticks: list, period: int = 60) -> list[dict]:
    """Aggregate raw ticks [[timestamp, price], ...] into OHLC candles."""
    from collections import defaultdict
    buckets: dict[int, list[float]] = defaultdict(list)
    for tick in ticks:
        if isinstance(tick, (list, tuple)) and len(tick) >= 2:
            try:
                ts = float(tick[0])
                price = float(tick[1])
                bucket = int(ts // period) * period
                buckets[bucket].append(price)
            except (ValueError, TypeError):
                continue
    candles = []
    for bucket in sorted(buckets.keys()):
        prices = buckets[bucket]
        if prices:
            candles.append({
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
            })
    return candles


def _candles_from_binary_frames(
    binary_frames: list[tuple[str, bytes]],
    count: int,
    period: int = 60,
) -> list[dict]:
    """
    Extract candles from Socket.IO binary frames.
    Primary: 'updateHistoryNewFast' → ticks aggregated into OHLC.
    """
    best: list[dict] = []
    for event_name, data in binary_frames:
        if event_name != "updateHistoryNewFast":
            continue
        try:
            parsed = json.loads(data.decode("utf-8"))
        except Exception as e:
            logger.warning("Binary frame decode error: %s", e)
            continue
        history = parsed.get("history")
        asset = parsed.get("asset", "unknown")
        if not isinstance(history, list) or not history:
            continue
        candles = _ticks_to_candles(history, period=period)
        logger.info("Parsed %d candles from %s history (%s)", len(candles), asset, event_name)
        if len(candles) > len(best):
            best = candles
    result = best[-count:] if len(best) > count else best
    return result


# Legacy helpers kept for _extract_candles_from_dom (unused by get_candles but used in fallback)
def _try_parse_candle(item: Any) -> dict | None:
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


_trade_page = None  # kept alive during an active demo trade


async def place_demo_trade(symbol: str, direction: str, expiration_sec: int) -> None:
    """
    Open a $1 demo trade on Pocket Option in the given direction.
    Keeps _trade_page open so take_trade_result_screenshot can use it later.
    """
    global _trade_page

    context = await _get_context()

    # Close any previous trade page
    if _trade_page is not None:
        try:
            await _trade_page.close()
        except Exception:
            pass

    _trade_page = await context.new_page()
    try:
        await _ensure_logged_in(context, _trade_page)

        symbol_clean = symbol.lstrip("#")
        trade_url = f"{config.PO_BASE_URL}/en/cabinet/demo-quick-high-low/?asset=%23{symbol_clean}"
        await _trade_page.goto(trade_url, wait_until="networkidle", timeout=30_000)
        await _trade_page.wait_for_timeout(3000)

        # Set amount to $1
        amount_selectors = [
            '.blocks-bet__amount input',
            'input.amount-control',
            'input[name="amount"]',
            '.bet-inputs input[type="text"]',
            '.input-control--amount input',
        ]
        for sel in amount_selectors:
            try:
                el = _trade_page.locator(sel).first
                if await el.count() > 0:
                    await el.triple_click()
                    await el.fill("1")
                    logger.info("Amount set via: %s", sel)
                    break
            except Exception:
                continue

        await _trade_page.wait_for_timeout(300)

        # Click BUY or SELL
        if direction == "BUY":
            btn_selectors = [
                '.block-btns-bet__btn--call',
                '.btn-call',
                '[data-direction="call"]',
                'button:has-text("Выше")',
                'button:has-text("Higher")',
                'button:has-text("UP")',
                '.call-btn',
            ]
        else:
            btn_selectors = [
                '.block-btns-bet__btn--put',
                '.btn-put',
                '[data-direction="put"]',
                'button:has-text("Ниже")',
                'button:has-text("Lower")',
                'button:has-text("DOWN")',
                '.put-btn',
            ]

        clicked = False
        for sel in btn_selectors:
            try:
                btn = _trade_page.locator(sel).first
                if await btn.count() > 0:
                    await btn.wait_for(state="visible", timeout=3000)
                    await btn.click()
                    clicked = True
                    logger.info("Demo trade placed via %s (direction=%s)", sel, direction)
                    break
            except Exception:
                continue

        if not clicked:
            logger.warning("Could not find %s trade button — page left open for screenshot", direction)

    except Exception as e:
        logger.exception("place_demo_trade failed: %s", e)
        # Keep _trade_page open so we can still take a fallback screenshot


async def take_trade_result_screenshot(symbol: str, direction: str) -> str:
    """
    Take a screenshot of the closed trade result popup/notification.
    Uses the _trade_page kept open by place_demo_trade.
    Falls back to a new page screenshot if the trade page was lost.
    """
    global _trade_page

    symbol_clean = symbol.lstrip("#")
    fname = f"result_{symbol_clean}_{int(time.time())}.png"
    path = str(SCREENSHOTS_DIR / fname)

    page = _trade_page
    owns_page = False

    if page is None or page.is_closed():
        logger.warning("Trade page is gone — opening new page for fallback screenshot")
        context = await _get_context()
        page = await context.new_page()
        owns_page = True
        try:
            await _ensure_logged_in(context, page)
            trade_url = f"{config.PO_BASE_URL}/en/cabinet/demo-quick-high-low/?asset=%23{symbol_clean}"
            await page.goto(trade_url, wait_until="networkidle", timeout=30_000)
            await page.wait_for_timeout(3000)
        except Exception as e:
            logger.exception("Fallback page load failed: %s", e)

    try:
        # Wait a moment for the result popup to render
        await page.wait_for_timeout(2000)

        # Dump top-level classes to help identify selectors
        try:
            top_classes = await page.evaluate(
                "() => Array.from(document.querySelectorAll('[class]')).slice(0,40)"
                ".map(el => el.tagName + '.' + el.className.trim().split(' ').join('.')).join('\\n')"
            )
            logger.info("Page top-level elements:\n%s", top_classes)
        except Exception:
            pass

        # Try to find and click the most recent closed trade to open its result popup
        closed_deal_selectors = [
            '.deals-block .deal:first-child',
            '.finished-deals .item:first-child',
            '.closed-deals .deal:first-child',
            '.deals-list .deal--closed:first-child',
            '.trades-history .item:first-child',
            '.block-trades .trade:first-child',
            '[data-tab="closed"] .item:first-child',
            '.history-deals .item:first-child',
            '.orders-history .item:first-child',
        ]
        for sel in closed_deal_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    logger.info("Clicked closed trade via: %s", sel)
                    await page.wait_for_timeout(1500)
                    break
            except Exception:
                continue

        # Detect win/loss from DOM before screenshotting
        outcome = "unknown"
        try:
            outcome = await page.evaluate("""() => {
                const text = document.body.innerText.toLowerCase();
                const allClasses = Array.from(document.querySelectorAll('[class]'))
                    .map(el => el.className).join(' ');

                const winSelectors = [
                    '.notification--profit', '.result--win', '.deal--win',
                    '.trade--profit', '.win', '.profit'
                ];
                const lossSelectors = [
                    '.notification--loss', '.result--lose', '.deal--lose',
                    '.trade--loss', '.lose', '.loss'
                ];

                for (const sel of winSelectors) {
                    if (document.querySelector(sel)) return 'win';
                }
                for (const sel of lossSelectors) {
                    if (document.querySelector(sel)) return 'loss';
                }

                if (text.includes('выиграли') || text.includes('win') || text.includes('profit'))
                    return 'win';
                if (text.includes('проиграли') || text.includes('lose') || text.includes('loss'))
                    return 'loss';

                return 'unknown';
            }""")
            logger.info("Detected trade outcome: %s", outcome)
        except Exception as e:
            logger.warning("Could not detect outcome: %s", e)

        # Try to screenshot just the result popup/modal
        result_popup_selectors = [
            '.deal-popup',
            '.trade-popup',
            '.popup-result',
            '.deal-result',
            '.popup.active',
            '.modal--deal',
            '.result-popup',
            '.notification--profit',
            '.notification--loss',
            '.notification.active',
            '.alert--result',
        ]
        for sel in result_popup_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.wait_for(state="visible", timeout=3000)
                    await el.screenshot(path=path)
                    logger.info("Result popup screenshotted via: %s", sel)
                    return path, outcome
            except Exception:
                continue

        # Final fallback — screenshot the full viewport
        await page.screenshot(path=path, full_page=False)
        logger.info("Result screenshot saved (full page fallback): %s", path)
        return path, outcome

    except Exception as e:
        logger.exception("take_trade_result_screenshot failed: %s", e)
        raise
    finally:
        if owns_page:
            await page.close()
        _trade_page = None


async def take_screenshot(symbol: str) -> str:
    """Legacy screenshot — kept for compatibility."""
    path, _ = await take_trade_result_screenshot(symbol, direction="BUY")
    return path


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
