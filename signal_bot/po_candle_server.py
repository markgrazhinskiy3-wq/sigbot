"""
po_candle_server.py — VPS Candle Server
=========================================
Run this on your Aeza VPS.
It connects to PocketOption WebSocket, collects candles every 45 seconds,
and serves them via a simple HTTP API that your Railway bot reads from.

Setup:
  pip3 install aiohttp

Config (set as env vars or edit below):
  PO_SSID   = your session value from Railway env vars
  SERVER_PORT = 8765  (or any open port on your VPS)

Usage:
  PO_SSID="a:4:{...}" python3 po_candle_server.py

Then set in Railway:
  CANDLE_API_URL = http://<your-vps-ip>:8765
"""

import asyncio
import json
import logging
import os
import re
import time

from aiohttp import web
import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("candle-server")

PO_SSID     = os.environ.get("PO_SSID", "").strip()
PO_WS_URL   = os.environ.get("PO_WS_URL", "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket").strip()
SERVER_PORT = int(os.environ.get("SERVER_PORT", "8765"))
SESSION_FILE = os.path.join(os.path.dirname(__file__), "po_session.json")

PAIRS = [
    "#EURUSD_otc", "#EURGBP_otc", "#EURJPY_otc", "#GBPJPY_otc",
    "#AUDUSD_otc", "#NZDUSD_otc", "#USDCAD_otc", "#USDCHF_otc",
    "#GBPUSD_otc", "#USDJPY_otc", "#EURCAD_otc", "#EURCHF_otc",
    "#GBPCAD_otc", "#GBPCHF_otc", "#AUDCAD_otc", "#AUDCHF_otc",
    "#AUDNZD_otc", "#NZDCAD_otc", "#CADJPY_otc", "#CHFJPY_otc",
]

FETCH_INTERVAL = 45
HISTORY_TIMEOUT = 15
MIN_CANDLES = 10

_store: dict[str, dict] = {}


def parse_ssid(ssid: str) -> dict | None:
    ssid = ssid.strip().strip('"').strip("'")
    if ssid.startswith("42"):
        try:
            payload = json.loads(ssid[2:])
            if isinstance(payload, list) and len(payload) >= 2:
                auth = payload[1]
                if isinstance(auth, dict) and ("session" in auth or "sessionToken" in auth):
                    return auth
        except Exception:
            pass
    if ssid.startswith("{"):
        try:
            auth = json.loads(ssid)
            if isinstance(auth, dict) and ("session" in auth or "sessionToken" in auth):
                return auth
        except Exception:
            pass
    if ssid.startswith("a:"):
        return {"session": ssid, "isDemo": 0}
    return None


def _parse_php_session(auth: dict) -> dict:
    session = auth.get("session", "")
    if not session.startswith("a:"):
        return {}
    result = {}
    for key in ("session_id", "user_agent", "ip_address"):
        m = re.search(rf's:{len(key)}:"{key}";s:\d+:"([^"]+)"', session)
        if m:
            result[key] = m.group(1)
    return result


def session_cookie(auth: dict) -> str:
    fields = _parse_php_session(auth)
    sid = fields.get("session_id", "")
    return f"ci_session={sid}" if sid else ""


def session_ua(auth: dict) -> str:
    fields = _parse_php_session(auth)
    return fields.get("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")


def candles_from_binary(raw: bytes) -> list[dict]:
    try:
        text = raw.decode("utf-8", errors="ignore").lstrip("\x00 \t\r\n")
        data = json.loads(text)
        if not isinstance(data, list) or not data:
            return []
        rows = data[0] if isinstance(data[0], list) else data
        candles = []
        for row in rows:
            if isinstance(row, (list, tuple)) and len(row) >= 5:
                ts, o, c, h, l = row[0], row[1], row[2], row[3], row[4]
                candles.append({"time": int(ts), "open": float(o), "close": float(c),
                                 "high": float(h), "low": float(l)})
        return candles
    except Exception:
        return []


async def reader_task(ws: aiohttp.ClientWebSocketResponse, queue: asyncio.Queue) -> None:
    pending_event = None
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                text = msg.data
                if text.startswith("451-"):
                    try:
                        pending_event = text[4:].split('"')[1]
                    except Exception:
                        pending_event = None
                else:
                    await queue.put(("text", text))
            elif msg.type == aiohttp.WSMsgType.BINARY:
                if pending_event:
                    await queue.put(("binary", pending_event, msg.data))
                    pending_event = None
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                await queue.put(None)
                return
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Reader: %s", e)
        await queue.put(None)


async def keepalive_task(ws: aiohttp.ClientWebSocketResponse, interval: float = 25.0) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            await ws.send_str("2")
    except (asyncio.CancelledError, Exception):
        pass


async def fetch_all_candles(auth: dict) -> dict[str, list[dict]]:
    headers = {
        "Origin": "https://pocketoption.com",
        "Referer": "https://pocketoption.com/en/cabinet/demo-quick-high-low/",
        "User-Agent": session_ua(auth),
    }
    cookie = session_cookie(auth)
    if cookie:
        headers["Cookie"] = cookie

    results: dict[str, list[dict]] = {}

    async with aiohttp.ClientSession() as http:
        async with http.ws_connect(
            PO_WS_URL, headers=headers,
            heartbeat=None, receive_timeout=None,
            autoclose=False, autoping=False,
        ) as ws:
            queue: asyncio.Queue = asyncio.Queue()
            rt = asyncio.create_task(reader_task(ws, queue))
            kt = asyncio.create_task(keepalive_task(ws, 25.0))
            try:
                # EIO handshake
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=5.0)
                    if item and item[0] == "text" and item[1].startswith("0"):
                        info = json.loads(item[1][1:])
                        ping_iv = info.get("pingInterval", 25000) / 1000
                        kt.cancel()
                        kt = asyncio.create_task(keepalive_task(ws, ping_iv))
                except asyncio.TimeoutError:
                    pass

                await ws.send_str("40")

                try:
                    await asyncio.wait_for(queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

                full_auth = {**auth, "isFastHistory": False, "isOptimized": False}
                await ws.send_str("42" + json.dumps(["auth", full_auth]))
                for msg in ['42["indicator/load"]', '42["favorite/load"]', '42["price-alert/load"]']:
                    await ws.send_str(msg)

                # Wait for auth
                auth_done = False
                deadline = time.monotonic() + 10.0
                while time.monotonic() < deadline and not auth_done:
                    rem = deadline - time.monotonic()
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=min(rem, 1.0))
                    except asyncio.TimeoutError:
                        continue
                    if item is None:
                        logger.error("Server closed connection during auth — PO_SSID may be expired or IP-blocked")
                        return results
                    if item[0] == "text":
                        if any(k in item[1] for k in ("successauth", "updateBalance", "updateProfile")):
                            auth_done = True
                    elif item[0] == "binary":
                        if item[1] in ("successauth", "updateBalance", "successupdateBalance"):
                            auth_done = True

                if auth_done:
                    logger.info("Auth OK")
                else:
                    logger.warning("Auth not confirmed, proceeding anyway...")

                for symbol in PAIRS:
                    asset = symbol.lstrip("#")
                    await ws.send_str("42" + json.dumps(["changeSymbol", {"asset": asset, "period": 60}]))
                    await ws.send_str('42["ps"]')
                    await ws.send_str("42" + json.dumps(["loadHistoryPeriod", {
                        "asset": asset, "index": 0, "time": int(time.time()),
                        "period": 60, "count": 720,
                    }]))

                    candles: list[dict] = []
                    deadline2 = time.monotonic() + HISTORY_TIMEOUT
                    while time.monotonic() < deadline2:
                        rem = deadline2 - time.monotonic()
                        try:
                            item = await asyncio.wait_for(queue.get(), timeout=min(rem, 1.0))
                        except asyncio.TimeoutError:
                            if len(candles) >= MIN_CANDLES:
                                break
                            continue
                        if item is None:
                            logger.warning("WS closed while fetching %s", symbol)
                            results[symbol] = candles
                            return results
                        if item[0] == "binary" and item[1] in ("updateHistoryNewFast", "updateHistoryPeriod", "updateHistory"):
                            batch = candles_from_binary(item[2])
                            if batch:
                                candles.extend(batch)
                                candles = sorted({c["time"]: c for c in candles}.values(), key=lambda c: c["time"])
                                if item[1] == "updateHistoryPeriod" and len(candles) >= MIN_CANDLES:
                                    break

                    if candles:
                        results[symbol] = candles
                        logger.info("  %s → %d candles", symbol, len(candles))
                    else:
                        logger.warning("  %s → 0 candles", symbol)

            finally:
                rt.cancel()
                kt.cancel()

    return results


async def fetch_loop(auth: dict) -> None:
    first = True
    while True:
        try:
            if not first:
                await asyncio.sleep(FETCH_INTERVAL)
            first = False

            logger.info("Fetching candles from PocketOption...")
            t0 = time.time()
            results = await fetch_all_candles(auth)
            elapsed = round(time.time() - t0, 1)

            if results:
                now = time.time()
                for symbol, candles in results.items():
                    _store[symbol] = {"candles": candles, "updated_at": now}
                logger.info("Updated %d symbols in %.1fs", len(results), elapsed)
            else:
                logger.warning("No candles fetched (%.1fs)", elapsed)

        except Exception as e:
            logger.error("Fetch loop error: %s", e)
            await asyncio.sleep(15)


async def handle_candles_all(request: web.Request) -> web.Response:
    now = time.time()
    result = {}
    for symbol, entry in _store.items():
        result[symbol] = {
            "candles": entry["candles"],
            "age_seconds": round(now - entry["updated_at"]),
        }
    return web.json_response(result)


async def handle_candles_symbol(request: web.Request) -> web.Response:
    symbol = request.match_info["symbol"]
    entry = _store.get(symbol)
    if not entry:
        return web.json_response({"error": "not found"}, status=404)
    return web.json_response({
        "symbol": symbol,
        "candles": entry["candles"],
        "age_seconds": round(time.time() - entry["updated_at"]),
    })


async def handle_status(request: web.Request) -> web.Response:
    now = time.time()
    symbols = []
    for sym, entry in _store.items():
        symbols.append({"symbol": sym, "candles": len(entry["candles"]),
                        "age_seconds": round(now - entry["updated_at"])})
    return web.json_response({"symbols": len(_store), "pairs": symbols})


async def main() -> None:
    auth = None

    # Priority 1: po_session.json (created by po_login.py — bound to VPS IP)
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE) as f:
                auth = json.load(f)
            logger.info("Loaded session from %s (uid=%s)", SESSION_FILE, auth.get("uid", "?"))
        except Exception as e:
            logger.warning("Could not read %s: %s", SESSION_FILE, e)
            auth = None

    # Priority 2: PO_SSID env var
    if not auth and PO_SSID:
        auth = parse_ssid(PO_SSID)
        if auth:
            logger.info("Using PO_SSID from env var")
        else:
            logger.error("Could not parse PO_SSID env var")

    if not auth:
        logger.error("No session found. Run po_login.py first:")
        logger.error("  PO_LOGIN='you@email.com' PO_PASSWORD='pass' python3 po_login.py")
        return

    logger.info("Starting candle server on port %d", SERVER_PORT)
    logger.info("WS URL: %s", PO_WS_URL)

    app = web.Application()
    app.router.add_get("/api/candles", handle_candles_all)
    app.router.add_get("/api/candles/{symbol}", handle_candles_symbol)
    app.router.add_get("/api/status", handle_status)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", SERVER_PORT)
    await site.start()
    logger.info("HTTP server listening on 0.0.0.0:%d", SERVER_PORT)

    await fetch_loop(auth)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped.")
