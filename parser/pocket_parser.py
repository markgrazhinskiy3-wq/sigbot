import asyncio
import logging
import time
from datetime import date, datetime
from dataclasses import dataclass, field
from typing import Any

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

import config
from utils.date_parser import fmt

logger = logging.getLogger(__name__)

CACHE_TTL = config.CACHE_TTL

STAT_LABELS = [
    "CTR",
    "RTD",
    "FTD #",
    "Депозиты #",
    "FTD $",
    "Депозиты $",
    "Реги #",
    "Выводы $",
    "Сделки #",
    "Клиенты #",
]


@dataclass
class CacheEntry:
    data: dict[str, Any]
    timestamp: float = field(default_factory=time.monotonic)


_cache: dict[str, CacheEntry] = {}
_cache_lock = asyncio.Lock()

_browser: Browser | None = None
_context: BrowserContext | None = None
_playwright = None


async def _get_browser() -> tuple[Browser, BrowserContext]:
    global _browser, _context, _playwright
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
            locale="ru-RU",
        )
        logger.info("Browser launched")
    return _browser, _context


async def _login(page: Page) -> None:
    logger.info("Navigating to login page: %s", config.PP_LOGIN_URL)
    await page.goto(config.PP_LOGIN_URL, wait_until="networkidle", timeout=30_000)

    await page.wait_for_timeout(1000)

    email_selectors = [
        'input[type="email"]',
        'input[name="email"]',
        'input[name="login"]',
        'input[placeholder*="mail" i]',
        'input[placeholder*="логин" i]',
    ]
    password_selectors = [
        'input[type="password"]',
        'input[name="password"]',
        'input[placeholder*="пароль" i]',
    ]
    submit_selectors = [
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Войти")',
        'button:has-text("Login")',
        'button:has-text("Sign in")',
    ]

    email_input = None
    for sel in email_selectors:
        try:
            email_input = await page.wait_for_selector(sel, timeout=5000)
            if email_input:
                break
        except Exception:
            continue

    if not email_input:
        raise RuntimeError("Не найдено поле ввода логина на странице входа")

    await email_input.fill(config.PP_LOGIN)
    logger.debug("Filled login field")

    password_input = None
    for sel in password_selectors:
        try:
            password_input = page.locator(sel).first
            if await password_input.count() > 0:
                break
            password_input = None
        except Exception:
            continue

    if not password_input:
        raise RuntimeError("Не найдено поле пароля на странице входа")

    await password_input.fill(config.PP_PASSWORD)
    logger.debug("Filled password field")

    submit_btn = None
    for sel in submit_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                submit_btn = btn
                break
        except Exception:
            continue

    if not submit_btn:
        raise RuntimeError("Не найдена кнопка входа на странице входа")

    await submit_btn.click()
    logger.info("Submitted login form, waiting for navigation...")

    await page.wait_for_load_state("networkidle", timeout=30_000)
    await page.wait_for_timeout(2000)

    current_url = page.url
    logger.info("After login URL: %s", current_url)

    if "login" in current_url.lower():
        raise RuntimeError(
            "Не удалось войти — возможно, неверный логин или пароль."
        )

    logger.info("Login successful")


async def _is_logged_in(page: Page) -> bool:
    try:
        await page.goto(config.PP_DASHBOARD_URL, wait_until="networkidle", timeout=20_000)
        await page.wait_for_timeout(1000)
        return "login" not in page.url.lower()
    except Exception:
        return False


async def _set_date_range(page: Page, date_from: date, date_to: date) -> None:
    logger.info("Setting date range: %s — %s", fmt(date_from), fmt(date_to))

    date_from_str = fmt(date_from)
    date_to_str = fmt(date_to)

    date_btn_selectors = [
        '[class*="date-picker"]',
        '[class*="datepicker"]',
        '[class*="DatePicker"]',
        '[class*="date-range"]',
        '[class*="daterange"]',
        'button:has-text("Период")',
        'button:has-text("Дата")',
        '[data-testid*="date"]',
    ]

    date_btn = None
    for sel in date_btn_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                date_btn = el
                logger.debug("Found date picker via selector: %s", sel)
                break
        except Exception:
            continue

    if date_btn:
        await date_btn.click()
        await page.wait_for_timeout(800)

    from_input_selectors = [
        'input[placeholder*="от" i]',
        'input[placeholder*="from" i]',
        'input[placeholder*="начало" i]',
        'input[name*="from" i]',
        'input[name*="start" i]',
        '[class*="from"] input',
        '[class*="start"] input',
    ]

    from_input = None
    for sel in from_input_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                from_input = el
                break
        except Exception:
            continue

    if from_input:
        await from_input.triple_click()
        await from_input.type(date_from_str, delay=80)
        logger.debug("Entered from date: %s", date_from_str)

    to_input_selectors = [
        'input[placeholder*="до" i]',
        'input[placeholder*="to" i]',
        'input[placeholder*="конец" i]',
        'input[name*="to" i]',
        'input[name*="end" i]',
        '[class*="to"] input',
        '[class*="end"] input',
    ]

    to_input = None
    for sel in to_input_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                to_input = el
                break
        except Exception:
            continue

    if to_input:
        await to_input.triple_click()
        await to_input.type(date_to_str, delay=80)
        logger.debug("Entered to date: %s", date_to_str)

    apply_selectors = [
        'button:has-text("Применить")',
        'button:has-text("Apply")',
        'button:has-text("Поиск")',
        'button:has-text("Search")',
        'button:has-text("Показать")',
        'button[type="submit"]:visible',
        '[class*="apply"]',
        '[class*="search-btn"]',
    ]

    for sel in apply_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                await btn.click()
                logger.debug("Clicked apply button via: %s", sel)
                break
        except Exception:
            continue

    await page.wait_for_load_state("networkidle", timeout=20_000)
    await page.wait_for_timeout(2000)
    logger.info("Date range applied")


async def _extract_stats(page: Page) -> dict[str, Any]:
    logger.info("Extracting dashboard stats")

    await page.wait_for_timeout(1500)

    stats: dict[str, Any] = {}

    content = await page.content()

    stat_selectors = [
        '[class*="stat"]',
        '[class*="metric"]',
        '[class*="widget"]',
        '[class*="card"]',
        '[class*="kpi"]',
        '[class*="dashboard"]',
        'td',
    ]

    target_keys = {
        "ctr": "CTR",
        "rtd": "RTD",
        "ftd #": "FTD #",
        "ftd#": "FTD #",
        "депозиты #": "Депозиты #",
        "ftd $": "FTD $",
        "ftd$": "FTD $",
        "депозиты $": "Депозиты $",
        "реги #": "Реги #",
        "реги#": "Реги #",
        "выводы $": "Выводы $",
        "выводы$": "Выводы $",
        "сделки #": "Сделки #",
        "сделки#": "Сделки #",
        "клиенты #": "Клиенты #",
        "клиенты#": "Клиенты #",
    }

    page_text = await page.evaluate("document.body.innerText")
    lines = [line.strip() for line in page_text.split("\n") if line.strip()]

    for i, line in enumerate(lines):
        line_lower = line.lower().replace(" ", "")
        for key_lower, label in target_keys.items():
            key_no_space = key_lower.replace(" ", "")
            if key_no_space in line_lower and label not in stats:
                for j in range(i + 1, min(i + 5, len(lines))):
                    candidate = lines[j].strip()
                    if candidate and candidate not in [lbl for lbl in target_keys.values()]:
                        if any(c.isdigit() for c in candidate) or candidate in ("-", "0", "—"):
                            stats[label] = candidate
                            logger.debug("Found %s = %s", label, candidate)
                            break

    if len(stats) < 5:
        logger.warning(
            "Only %d stats found via text parsing. Trying JS evaluation.", len(stats)
        )
        try:
            js_stats = await page.evaluate(
                """() => {
                    const result = {};
                    const allElements = document.querySelectorAll('*');
                    const labels = ['CTR','RTD','FTD #','Депозиты #','FTD $','Депозиты $','Реги #','Выводы $','Сделки #','Клиенты #'];
                    for (const el of allElements) {
                        const text = el.textContent.trim();
                        for (const label of labels) {
                            if (text === label && !(label in result)) {
                                const parent = el.parentElement;
                                if (parent) {
                                    const siblings = parent.querySelectorAll('*');
                                    for (const sib of siblings) {
                                        const sibText = sib.textContent.trim();
                                        if (sibText !== label && /[\\d.,%-$]/.test(sibText) && sibText.length < 20) {
                                            result[label] = sibText;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return result;
                }"""
            )
            stats.update({k: v for k, v in js_stats.items() if k not in stats})
        except Exception as e:
            logger.error("JS evaluation failed: %s", e)

    for label in STAT_LABELS:
        if label not in stats:
            stats[label] = "—"

    logger.info("Extracted %d stats: %s", len([v for v in stats.values() if v != "—"]), stats)
    return stats


async def parse_dashboard_stats(date_from: date, date_to: date) -> dict[str, Any]:
    cache_key = f"{fmt(date_from)}_{fmt(date_to)}"

    async with _cache_lock:
        entry = _cache.get(cache_key)
        if entry and (time.monotonic() - entry.timestamp) < CACHE_TTL:
            logger.info("Cache hit for %s", cache_key)
            return entry.data

    _, context = await _get_browser()

    page = await context.new_page()
    try:
        logged_in = await _is_logged_in(page)
        if not logged_in:
            logger.info("Not logged in, performing login")
            await _login(page)
            await page.goto(config.PP_DASHBOARD_URL, wait_until="networkidle", timeout=30_000)
            await page.wait_for_timeout(2000)

        await _set_date_range(page, date_from, date_to)

        stats = await _extract_stats(page)

        async with _cache_lock:
            _cache[cache_key] = CacheEntry(data=stats)

        return stats

    except Exception as e:
        logger.exception("Error during dashboard parsing: %s", e)
        try:
            screenshot_path = f"/tmp/error_{int(time.time())}.png"
            await page.screenshot(path=screenshot_path, full_page=True)
            logger.info("Error screenshot saved to %s", screenshot_path)
        except Exception:
            pass
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
    logger.info("Browser closed")
