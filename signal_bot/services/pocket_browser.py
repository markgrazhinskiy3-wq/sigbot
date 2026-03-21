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


async def _switch_to_asset(page: Page, symbol: str) -> bool:
    """
    Switch the Pocket Option platform to the given asset via UI.
    Returns True if the asset was confirmed/switched successfully.
    """
    symbol_clean = symbol.lstrip("#")
    is_otc = "_otc" in symbol_clean.lower()
    base = symbol_clean.replace("_otc", "").replace("_OTC", "").upper()
    slash_base = f"{base[:3]}/{base[3:]}" if len(base) == 6 else base
    slash_name = f"{slash_base} OTC" if is_otc else slash_base  # e.g. "EUR/USD OTC"

    # Check if asset is already correct
    try:
        current = await page.evaluate("""() => {
            const sels = [
                '.currencies-block',
                '.block-active-asset-name',
                '.header-main__asset .name',
                '.asset-name',
                '.current-pair',
                '.current-symbol',
                '[class*="asset-name"]',
                '[class*="active-asset"]',
            ];
            for (const s of sels) {
                const el = document.querySelector(s);
                if (el && el.textContent.trim()) return el.textContent.trim();
            }
            return '';
        }""")
        if current and slash_name.replace("/", "").replace(" ", "").lower() in current.replace("/", "").replace(" ", "").lower():
            logger.info("Asset already correct: %s", current)
            return False  # No switch needed — callers must NOT reload
        logger.info("Current asset: '%s', need: '%s' — switching", current, slash_name)
    except Exception:
        pass

    # Step 1: Get bounding box of the asset name element and use REAL mouse click
    # JS el.click() doesn't fire Vue.js handlers — page.mouse.click() does
    try:
        box = await page.evaluate(f"""() => {{
            const text = {repr(current)};
            const all = Array.from(document.querySelectorAll('*'));
            // Find element near top of page (y < 120px) — the asset HEADER button,
            // not the trade history entries which are further down the page
            for (const el of all) {{
                const r = el.getBoundingClientRect();
                if (el.textContent.trim() === text &&
                    el.offsetWidth > 0 && el.offsetHeight > 0 &&
                    r.top < 120 && r.top >= 0) {{
                    // Walk up to the first element with cursor:pointer or button
                    let target = el;
                    for (let i = 0; i < 8; i++) {{
                        const s = window.getComputedStyle(target);
                        if (target.tagName === 'BUTTON' || target.tagName === 'A' ||
                            target.getAttribute('role') === 'button' ||
                            s.cursor === 'pointer') {{
                            const r2 = target.getBoundingClientRect();
                            return {{x: r2.left + r2.width/2, y: r2.top + r2.height/2, sel: target.className}};
                        }}
                        if (!target.parentElement) break;
                        target = target.parentElement;
                    }}
                    return {{x: r.left + r.width/2, y: r.top + r.height/2, sel: el.className}};
                }}
            }}
            return null;
        }}""")

        if box:
            logger.info("Asset element found at (%.0f, %.0f) class='%s' — mouse clicking", box['x'], box['y'], box.get('sel', ''))
            await page.mouse.click(box['x'], box['y'])
            await page.wait_for_timeout(1500)
        else:
            logger.warning("Asset element bounding box not found")
    except Exception as e:
        logger.warning("Mouse click on asset failed: %s", e)

    # Step 2: Take debug screenshot to see if panel opened
    try:
        dbg_path = str(SCREENSHOTS_DIR / "asset_panel_debug.png")
        await page.screenshot(path=dbg_path, full_page=False)
        logger.info("Asset panel debug screenshot: %s", dbg_path)
    except Exception:
        pass

    # Step 3: Check if a search input appeared (panel opened successfully)
    panel_open = False
    input_sel = None
    for sel in [
        'input[placeholder*="earch" i]',
        'input[placeholder*="оиск" i]',
        '.assets-search__input',
        '.search-assets input',
        '.assets-filter input',
    ]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                panel_open = True
                input_sel = sel
                logger.info("Asset panel opened — search input found via: %s", sel)
                break
        except Exception:
            continue

    # Step 4: If panel not opened via mouse click, try CSS selectors
    if not panel_open:
        for sel in [
            '.block-active-asset-name',
            '.header-main__asset',
            '.assets-toggle',
            '.asset-select-btn',
            '.open-assets',
            '[data-action="open-assets"]',
            '.instrument-name',
            '.header__asset',
            '.chart-header__asset',
            '.trade-asset',
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click(timeout=3000)
                    logger.info("Opened asset panel via CSS: %s", sel)
                    await page.wait_for_timeout(1500)
                    # Check again for search input
                    for isel in ['input[placeholder*="earch" i]', 'input[placeholder*="оиск" i]', 'input[type="search"]']:
                        try:
                            if await page.locator(isel).count() > 0:
                                panel_open = True
                                input_sel = isel
                                break
                        except Exception:
                            continue
                    if panel_open:
                        break
            except Exception:
                continue

    if not panel_open:
        logger.warning("Asset selector panel did not open for %s", slash_name)
        return False

    # Step 5: Type in the search box
    # The input may be hidden by CSS — use mouse coordinates to click, not Playwright visibility click
    typed = False
    try:
        search_box = await page.evaluate("""() => {
            // Find specifically the search/filter input (not Amount or other inputs)
            const sel = 'input[placeholder*="earch"], input[placeholder*="оиск"], input[placeholder*="Search"], input[placeholder*="Filter"]';
            const el = document.querySelector(sel);
            if (el) {
                const r = el.getBoundingClientRect();
                return {x: r.left + r.width/2, y: r.top + r.height/2, ph: el.placeholder};
            }
            return null;
        }""")
        if search_box and search_box.get('y', 0) > 0:
            await page.mouse.click(search_box['x'], search_box['y'])
            await page.wait_for_timeout(200)
            await page.keyboard.type(slash_name)
            logger.info("Typed '%s' via mouse+keyboard (coords: %.0f,%.0f, placeholder='%s')", slash_name, search_box['x'], search_box['y'], search_box.get('ph', ''))
            await page.wait_for_timeout(800)
            typed = True
    except Exception as e:
        logger.warning("Mouse+keyboard type failed: %s", e)

    if not typed:
        # Fallback: just keyboard.type in case search field is already focused
        try:
            await page.keyboard.type(slash_name)
            logger.info("Typed '%s' via keyboard fallback", slash_name)
            await page.wait_for_timeout(800)
            typed = True
        except Exception as e:
            logger.warning("Keyboard fallback also failed: %s", e)
            return False

    # Step 6: Click the matching asset in results
    for sel in [
        f'.asset-item:has-text("{slash_name}")',
        f'.assets-item:has-text("{slash_name}")',
        f'.item:has-text("{slash_name}")',
        f'[data-asset="#{symbol_clean}"]',
        f'[data-id="#{symbol_clean}"]',
        f'[data-symbol="{symbol}"]',
        '.asset-item:first-child',
        '.assets-item:first-child',
        'li.asset:first-child',
    ]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click()
                logger.info("Selected asset via: %s", sel)
                await page.wait_for_timeout(2000)
                return True
        except Exception:
            continue

    # Try clicking first visible result after search
    try:
        results = page.locator(f'text="{slash_name}"')
        cnt = await results.count()
        if cnt > 0:
            await results.first.click()
            logger.info("Clicked text match for '%s'", slash_name)
            await page.wait_for_timeout(2000)
            return True
    except Exception:
        pass

    logger.warning("Could not select asset %s from panel", slash_name)
    return False


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
        trade_url = f"{config.PO_BASE_URL}/en/cabinet/demo-quick-high-low/?asset=%23{symbol_clean}"
        await page.goto(trade_url, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(2000)

        # Switch to the correct asset via UI if the platform defaulted to another pair
        switched = await _switch_to_asset(page, symbol)
        if switched:
            # After UI switch PocketOption remembers the selected asset.
            # Reload the page so the WS re-subscribes to the new asset from scratch.
            binary_frames.clear()
            logger.info("Asset switched — reloading page so WS subscribes to %s", symbol)
            await page.goto(trade_url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(3000)
        
        # Verify we're on the correct asset now
        try:
            current_asset = await page.evaluate("""() => {
                const sels = ['.currencies-block', '.block-active-asset-name', '.asset-name', '[class*="active-asset"]'];
                for (const s of sels) {
                    const el = document.querySelector(s);
                    if (el && el.textContent.trim()) return el.textContent.trim();
                }
                return null;
            }""")
            logger.info("Asset after load: '%s'", current_asset)
        except Exception:
            pass

        # Wait for fresh history data (up to 35s)
        deadline = 35
        for _ in range(deadline * 2):
            await page.wait_for_timeout(500)
            if any(ev == "updateHistoryNewFast" for ev, _ in binary_frames):
                break
        
        # Log what WS data arrived (all assets) for debugging
        seen_assets = list({
            json.loads(d.decode("utf-8")).get("asset", "?")
            for ev, d in binary_frames if ev == "updateHistoryNewFast"
            if _safe_json_asset(d)
        })
        logger.info("WS assets received: %s", seen_assets)

        candles = _candles_from_binary_frames(binary_frames, count, symbol=symbol, period=30)
        
        # Fallback: if 0 candles for target symbol, try any symbol (in case asset name format differs)
        if len(candles) == 0 and binary_frames:
            logger.warning("No candles for %s — falling back to any received WS data", symbol)
            candles = _candles_from_binary_frames(binary_frames, count, symbol="", period=30)
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


def _safe_json_asset(data: bytes) -> str | None:
    """Safely decode a binary WS frame and return its 'asset' field, or None."""
    try:
        return json.loads(data.decode("utf-8")).get("asset")
    except Exception:
        return None


def _candles_from_binary_frames(
    binary_frames: list[tuple[str, bytes]],
    count: int,
    symbol: str = "",
    period: int = 60,
) -> list[dict]:
    """
    Extract candles from Socket.IO binary frames.
    Primary: 'updateHistoryNewFast' → ticks aggregated into OHLC.
    Filters by symbol if provided (e.g. '#EURUSD_otc' → 'EURUSD_otc').
    """
    symbol_clean = symbol.lstrip("#").upper() if symbol else ""
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
        # Filter to matching symbol — accept if symbol not specified or asset matches
        if symbol_clean and asset.upper() != symbol_clean:
            logger.debug("Skipping WS frame for %s (want %s)", asset, symbol_clean)
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
        await _trade_page.wait_for_timeout(2000)

        # Switch to the correct asset via UI if the platform defaulted to another pair
        trade_switched = await _switch_to_asset(_trade_page, symbol)
        if trade_switched:
            # Reload so the platform starts fresh on the newly selected asset
            logger.info("Trade page: asset switched, reloading to confirm %s", symbol)
            await _trade_page.goto(trade_url, wait_until="domcontentloaded", timeout=30_000)
            await _trade_page.wait_for_timeout(2000)
        else:
            await _trade_page.wait_for_timeout(1000)

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


async def take_trade_result_screenshot(
    symbol: str,
    direction: str,
    placed_at: float = 0,
    expiration_sec: int = 0,
) -> str:
    """
    Take a screenshot of the closed trade result popup/notification.
    Uses the _trade_page kept open by place_demo_trade.
    Falls back to a new page screenshot if the trade page was lost.

    placed_at      — unix timestamp when the trade was placed (for time matching)
    expiration_sec — trade duration, so we can compute expected close HH:MM
    """
    global _trade_page

    symbol_clean = symbol.lstrip("#")
    fname = f"result_{symbol_clean}_{int(time.time())}.png"
    path = str(SCREENSHOTS_DIR / fname)

    # Build a list of HH:MM strings the closed trade might show (try UTC+0..+5)
    # The platform uses UTC+3 based on observed screenshots, but we try a range
    # so the code stays correct if the user's account is in a different timezone.
    import datetime as _dt
    close_time_candidates: list[str] = []
    if placed_at and expiration_sec:
        close_ts = int(placed_at) + expiration_sec
        for offset_h in range(0, 13):  # UTC+0 … UTC+12
            d = _dt.datetime.utcfromtimestamp(close_ts + offset_h * 3600)
            close_time_candidates.append(f"{d.hour:02d}:{d.minute:02d}")
        logger.info("Expected close time candidates: %s", close_time_candidates)

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

        # Detect win/loss by looking for the most recent closed trade entry.
        # PocketOption shows "+$X.XX" for wins and "$0" for losses in Closed panel.
        # We pass the pair label (e.g. "NZD/USD OTC") to find OUR trade specifically.
        is_otc = "_otc" in symbol_clean.lower()
        base = symbol_clean.replace("_otc", "").replace("_OTC", "").upper()
        slash_base = f"{base[:3]}/{base[3:]}" if len(base) == 6 else base
        pair_label_js = f"{slash_base} OTC" if is_otc else slash_base

        close_times_js = repr(close_time_candidates)

        # JS that searches ONLY for our specific pair — returns JSON or null (not found yet)
        pair_detect_js = f"""() => {{
            const pairLabel  = {repr(pair_label_js)};
            const closeTimes = {close_times_js};
            const hasProfit  = (t) => /\\+\\s*\\$[\\d.]+/.test(t);
            const hasZero    = (t) => /\\$\\s*0(\\.00?)?\\b/.test(t);
            const hasTime    = (t) => closeTimes.length > 0 && closeTimes.some(hm => t.includes(hm));

            // CSS popup classes — fastest check
            const strictWin  = ['.notification--profit', '.result--win', '.deal--win', '.trade--profit'];
            const strictLoss = ['.notification--loss',  '.result--lose', '.deal--lose', '.trade--loss'];
            for (const s of strictWin)  {{ if (document.querySelector(s)) return JSON.stringify({{outcome:'win',  step:1}}); }}
            for (const s of strictLoss) {{ if (document.querySelector(s)) return JSON.stringify({{outcome:'loss', step:1}}); }}

            // Closed trades always show a HH:MM timestamp; open trades do not.
            // This prevents matching an open trade card showing $0.
            const hasCloseTime = (t) => /\\d{{2}}:\\d{{2}}/.test(t);

            // Walk elements: must contain our pair + profit/loss + HH:MM + be card-sized
            const candidates = [];
            for (const el of document.querySelectorAll('*')) {{
                if (el.children.length > 8) continue;
                const t = (el.innerText || '').trim();
                if (!t.includes(pairLabel)) continue;
                if (!hasCloseTime(t)) continue;           // ← skip open trades (no HH:MM)
                if (t.includes('Payout') || t.includes('payout')) continue;
                if (!hasProfit(t) && !hasZero(t)) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 80 || r.height < 20 || r.height > 120) continue;
                if (r.width * r.height > 120000) continue;
                candidates.push({{ t, y: r.top, hasOurTime: hasTime(t) }});
            }}
            if (candidates.length === 0) return null;  // not ready yet — caller retries

            const withTime = candidates.filter(c => c.hasOurTime);
            // When we know the expected close time, require a match —
            // never fall back to an old unmatched trade card.
            if (closeTimes.length > 0 && withTime.length === 0) return null;
            const pool = withTime.length > 0 ? withTime : candidates;
            pool.sort((a, b) => a.y - b.y);
            const top     = pool[0];
            const outcome = hasProfit(top.t) ? 'win' : hasZero(top.t) ? 'loss' : null;
            if (!outcome) return null;
            return JSON.stringify({{
                outcome,
                total: candidates.length,
                timeMatched: withTime.length > 0,
                topY: Math.round(top.y),
                text: top.t.replace(/\\s+/g, ' ').slice(0, 100),
            }});
        }}"""

        import json as _json
        outcome = "unknown"
        MAX_RETRIES = 15        # up to 15 × 2 s = 30 extra seconds
        RETRY_INTERVAL_MS = 2000

        for attempt in range(MAX_RETRIES):
            try:
                raw = await page.evaluate(pair_detect_js)
                if raw is not None:
                    info = _json.loads(raw)
                    outcome = info.get("outcome", "unknown")
                    logger.info(
                        "Detected trade outcome: %s | attempt=%d candidates=%s "
                        "timeMatched=%s topY=%s text=%r",
                        outcome, attempt + 1,
                        info.get("total"), info.get("timeMatched"),
                        info.get("topY"), info.get("text", ""),
                    )
                    break   # found — stop retrying
                else:
                    logger.debug("Attempt %d: trade card not in Closed panel yet", attempt + 1)
            except Exception as e:
                logger.warning("Detect attempt %d failed: %s", attempt + 1, e)

            if attempt < MAX_RETRIES - 1:
                await page.wait_for_timeout(RETRY_INTERVAL_MS)

        # Absolute last resort (no pair filter) — only if all retries exhausted
        if outcome == "unknown":
            logger.warning("Pair-specific detection failed after %d attempts — using last-resort fallback", MAX_RETRIES)
            try:
                outcome = await page.evaluate(f"""() => {{
                    const hasProfit = (t) => /\\+\\s*\\$[\\d.]+/.test(t);
                    const hasZero   = (t) => /\\$\\s*0(\\.00?)?\\b/.test(t);
                    const els = Array.from(document.querySelectorAll(
                        '[class*="trade"], [class*="deal"], [class*="history"], [class*="closed"]'
                    ));
                    const pool = [];
                    for (const el of els) {{
                        const t = el.innerText || '';
                        if (t.includes('Payout') || t.includes('payout')) continue;
                        if (!hasProfit(t) && !hasZero(t)) continue;
                        const r = el.getBoundingClientRect();
                        if (r.width < 80 || r.height < 20 || r.height > 120) continue;
                        pool.push({{ t, y: r.top }});
                    }}
                    if (pool.length > 0) {{
                        pool.sort((a, b) => a.y - b.y);
                        const top = pool[0];
                        if (hasProfit(top.t)) return 'win';
                        if (hasZero(top.t))   return 'loss';
                    }}
                    if (/проиграли|you lose|trade lost/i.test(document.body.innerText)) return 'loss';
                    return 'unknown';
                }}""")
                logger.info("Last-resort outcome: %s", outcome)
            except Exception as e:
                logger.warning("Last-resort detection failed: %s", e)

        # ── Priority 1: Screenshot the specific closed-trade card for our pair ──
        # Find the element that contains the pair label and scroll it into view,
        # then clip the screenshot to just that card (+ generous padding).
        PADDING = 12
        clipped = False
        try:
            clip_box = await page.evaluate(f"""() => {{
                const label = {repr(pair_label_js)};
                const hasProfit = (t) => /[+-]\\s*\\$[\\d.]+/.test(t);
                const hasZero   = (t) => /\\$\\s*0(\\.00?)?\\b/.test(t);

                // Collect all candidate elements that contain our pair label
                // and a profit/loss value. Pick the TOPMOST one (smallest Y) —
                // that is the most recently closed trade in the Closed panel
                // (PocketOption renders newest trades at the top of the list).
                const closeTimes   = {close_times_js};
                const hasTime      = (t) => closeTimes.length > 0 && closeTimes.some(hm => t.includes(hm));
                const hasCloseTime = (t) => /\\d{{2}}:\\d{{2}}/.test(t); // HH:MM means closed trade
                const candidates = [];
                for (const el of document.querySelectorAll('*')) {{
                    if (el.children.length > 8) continue;
                    if (!el.innerText) continue;
                    const t = el.innerText.trim();
                    if (!t.includes(label)) continue;
                    if (!hasCloseTime(t)) continue;      // ← skip open trades (no HH:MM yet)
                    if (t.includes('Payout') || t.includes('payout')) continue;
                    if (!hasProfit(t) && !hasZero(t)) continue;
                    const r = el.getBoundingClientRect();
                    const area = r.width * r.height;
                    if (r.width < 80 || r.height < 20) continue;
                    if (r.height > 120) continue;        // ← skip multi-row containers
                    if (area > 120000) continue;
                    candidates.push({{ r, y: r.top, hasOurTime: hasTime(t) }});
                }}
                if (candidates.length === 0) return null;
                // Require time match when we know the expected close time (prevents old trade cards)
                const withTime = candidates.filter(c => c.hasOurTime);
                if (closeTimes.length > 0 && withTime.length === 0) return null;
                const pool = withTime.length > 0 ? withTime : candidates;
                pool.sort((a, b) => a.y - b.y);
                const best = pool[0].r;
                return {{ x: best.x, y: best.y, width: best.width, height: best.height }};
            }}""")

            if clip_box and clip_box['width'] > 0 and clip_box['height'] > 0:
                pad = PADDING
                vw = page.viewport_size['width']  if page.viewport_size else 1440
                vh = page.viewport_size['height'] if page.viewport_size else 900
                clip = {
                    "x":      max(0, clip_box['x'] - pad),
                    "y":      max(0, clip_box['y'] - pad),
                    "width":  min(vw, clip_box['width']  + pad * 2),
                    "height": min(vh, clip_box['height'] + pad * 2),
                }
                await page.screenshot(path=path, clip=clip)
                logger.info(
                    "Clipped screenshot of closed trade card (%dx%d px)",
                    clip['width'], clip['height']
                )
                clipped = True
        except Exception as e:
            logger.warning("Clipped screenshot failed: %s", e)

        if clipped:
            return path, outcome

        # ── Priority 2: popup/notification elements (win/loss overlay) ──
        result_popup_selectors = [
            '.notification--profit', '.notification--loss', '.notification.active',
            '.deal-popup', '.trade-popup', '.popup-result',
            '.deal-result', '.popup.active', '.modal--deal',
            '.result-popup', '.alert--result',
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

        # ── Priority 3: screenshot just the right trades sidebar ──
        try:
            sidebar_box = await page.evaluate("""() => {
                const sels = ['.trades', '[class*="sidebar-right"]', '[class*="trades-panel"]'];
                for (const s of sels) {
                    const el = document.querySelector(s);
                    if (el) {
                        const r = el.getBoundingClientRect();
                        if (r.width > 0) return {x: r.x, y: r.y, width: r.width, height: r.height};
                    }
                }
                return null;
            }""")
            if sidebar_box and sidebar_box['width'] > 0:
                vw = page.viewport_size['width']  if page.viewport_size else 1440
                vh = page.viewport_size['height'] if page.viewport_size else 900
                clip = {
                    "x": max(0, sidebar_box['x']),
                    "y": 0,
                    "width":  min(vw - sidebar_box['x'], sidebar_box['width']),
                    "height": vh,
                }
                await page.screenshot(path=path, clip=clip)
                logger.info("Sidebar screenshotted (%dx%d px)", clip['width'], clip['height'])
                return path, outcome
        except Exception as e:
            logger.warning("Sidebar screenshot failed: %s", e)

        # ── Final fallback: full viewport ──
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
