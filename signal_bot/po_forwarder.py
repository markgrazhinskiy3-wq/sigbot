"""
po_forwarder.py — Home Machine Candle Forwarder
================================================
Run this on your home PC/NAS (where PocketOption is accessible).
It connects to PocketOption WS, collects candles, and pushes them
to your Railway API server every 45 seconds.

Setup:
  pip install aiohttp

Config (edit the CONSTANTS section below or set env vars):
  PO_SSID       = your PO_SSID string (same as Railway env var)
  PO_WS_URL     = wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket
  API_URL       = https://your-api-server.up.railway.app  (NO trailing slash)
  API_KEY       = your CANDLE_PUSH_KEY (set same value in Railway)

Usage:
  python3 po_forwarder.py
"""

import asyncio
import json
import logging
import os
import re
import sys
import time

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("forwarder")

# ── CONFIG (override via env vars or edit directly) ────────────────────────────
PO_SSID   = os.environ.get("PO_SSID",   "").strip()
PO_WS_URL = os.environ.get("PO_WS_URL", "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket").strip()
API_URL   = os.environ.get("API_URL",   "").strip().rstrip("/")
API_KEY   = os.environ.get("API_KEY",   "").strip()

# OTC pairs to fetch (must match your Railway bot config)
PAIRS = [
    "#EURUSD_otc", "#EURGBP_otc", "#EURJPY_otc", "#GBPJPY_otc",
    "#AUDUSD_otc", "#NZDUSD_otc", "#USDCAD_otc", "#USDCHF_otc",
    "#GBPUSD_otc", "#USDJPY_otc", "#EURCAD_otc", "#EURCHF_otc",
    "#GBPCAD_otc", "#GBPCHF_otc", "#AUDCAD_otc", "#AUDCHF_otc",
    "#AUDNZD_otc", "#NZDCAD_otc", "#CADJPY_otc", "#CHFJPY_otc",
]

PUSH_INTERVAL = 45  # seconds between pushes
HISTORY_TIMEOUT = 15  # seconds to wait for candles per symbol
MIN_CANDLES = 10


# ── Parse PO_SSID ──────────────────────────────────────────────────────────────
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
        return {"session": ssid, "isDemo": 1}
    return None


# ── PHP session helpers ────────────────────────────────────────────────────────
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
    return fields.get(
        "user_agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    )


# ── WebSocket candle fetcher ───────────────────────────────────────────────────
async def _reader(ws: aiohttp.ClientWebSocketResponse, queue: asyncio.Queue) -> None:
    pending_event: str | None = None
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                text: str = msg.data
                if text.startswith("451-"):
                    pending_event = text[4:].split('"')[1] if '"' in text[4:] else None
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
        logger.error("Reader error: %s", e)
        await queue.put(None)


async def _keepalive(ws: aiohttp.ClientWebSocketResponse, interval: float = 25.0) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            await ws.send_str("2")
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


def _candles_from_binary(raw: bytes) -> list[dict]:
    try:
        text = raw.decode("utf-8", errors="ignore").lstrip("\x00 \t\r\n")
        data = json.loads(text)
        if not isinstance(data, list) or not data:
            return []
        # unwrap nested arrays
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


async def fetch_candles(auth: dict, symbols: list[str]) -> dict[str, list[dict]]:
    headers = {
        "Origin": "https://pocketoption.com",
        "Referer": "https://pocketoption.com/en/cabinet/demo-quick-high-low/",
        "User-Agent": session_ua(auth),
    }
    cookie = session_cookie(auth)
    if cookie:
        headers["Cookie"] = cookie

    results: dict[str, list[dict]] = {}

    logger.info("Connecting to PO WebSocket...")
    async with aiohttp.ClientSession() as http:
        async with http.ws_connect(
            PO_WS_URL, headers=headers,
            heartbeat=None, receive_timeout=None,
            autoclose=False, autoping=False,
        ) as ws:
            queue: asyncio.Queue = asyncio.Queue()
            reader_task = asyncio.create_task(_reader(ws, queue))
            ping_task = asyncio.create_task(_keepalive(ws, 25.0))
            try:
                # EIO handshake
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=5.0)
                    if item and item[0] == "text" and item[1].startswith("0"):
                        info = json.loads(item[1][1:])
                        ping_interval = info.get("pingInterval", 25000) / 1000
                        ping_task.cancel()
                        ping_task = asyncio.create_task(_keepalive(ws, ping_interval))
                except asyncio.TimeoutError:
                    logger.warning("Timed out waiting for EIO OPEN")

                await ws.send_str("40")

                try:
                    ack = await asyncio.wait_for(queue.get(), timeout=5.0)
                    if ack and ack[0] == "text" and ack[1].startswith("40"):
                        logger.info("Namespace connected")
                except asyncio.TimeoutError:
                    logger.warning("Timed out waiting for namespace ACK")

                # Auth
                full_auth = {**auth, "isFastHistory": False, "isOptimized": False}
                await ws.send_str("42" + json.dumps(["auth", full_auth]))
                for msg in ['42["indicator/load"]', '42["favorite/load"]', '42["price-alert/load"]']:
                    await ws.send_str(msg)

                # Wait for auth confirmation
                auth_done = False
                deadline = time.monotonic() + 10.0
                while time.monotonic() < deadline and not auth_done:
                    rem = deadline - time.monotonic()
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=min(rem, 1.0))
                    except asyncio.TimeoutError:
                        continue
                    if item is None:
                        logger.error("Server closed connection during auth — check PO_SSID IP")
                        return results
                    if item[0] == "text":
                        text = item[1]
                        if any(k in text for k in ("successauth", "updateBalance", "updateProfile")):
                            auth_done = True
                            logger.info("Auth confirmed")
                    elif item[0] == "binary":
                        if item[1] in ("successauth", "updateBalance", "successupdateBalance"):
                            auth_done = True
                            logger.info("Auth confirmed (binary)")

                if not auth_done:
                    logger.warning("Auth not confirmed, proceeding anyway...")

                # Fetch candles for each symbol
                for symbol in symbols:
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
                            batch = _candles_from_binary(item[2])
                            if batch:
                                candles.extend(batch)
                                candles = sorted({c["time"]: c for c in candles}.values(), key=lambda c: c["time"])
                                if item[1] == "updateHistoryPeriod" and len(candles) >= MIN_CANDLES:
                                    break

                    if candles:
                        results[symbol] = candles
                        logger.info("  %s → %d candles", symbol, len(candles))
                    else:
                        logger.warning("  %s → no candles", symbol)

            finally:
                reader_task.cancel()
                ping_task.cancel()

    return results


# ── Push to API server ─────────────────────────────────────────────────────────
async def push_candles(candles: dict[str, list[dict]]) -> bool:
    if not API_URL:
        logger.error("API_URL not set — cannot push candles")
        return False

    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["x-push-key"] = API_KEY

    payload = {"batch": candles}

    try:
        async with aiohttp.ClientSession() as http:
            async with http.post(
                f"{API_URL}/api/candles",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                body = await resp.json()
                if resp.status == 200 and body.get("ok"):
                    logger.info("Pushed %d symbols to Railway API ✓", body.get("stored", 0))
                    return True
                logger.error("Push failed: %d %s", resp.status, body)
                return False
    except Exception as e:
        logger.error("Push error: %s", e)
        return False


# ── Main loop ──────────────────────────────────────────────────────────────────
async def main() -> None:
    if not PO_SSID:
        logger.error("PO_SSID is not set. Set it as env var or edit this script.")
        logger.error("Example: PO_SSID='a:4:{...}' python3 po_forwarder.py")
        sys.exit(1)

    if not API_URL:
        logger.error("API_URL is not set.")
        logger.error("Example: API_URL='https://your-server.up.railway.app' python3 po_forwarder.py")
        sys.exit(1)

    auth = parse_ssid(PO_SSID)
    if not auth:
        logger.error("Could not parse PO_SSID. Use the value from Railway env vars.")
        sys.exit(1)

    logger.info("Starting PO candle forwarder")
    logger.info("WS URL: %s", PO_WS_URL)
    logger.info("API URL: %s", API_URL)
    logger.info("Pairs: %d", len(PAIRS))

    while True:
        try:
            t0 = time.time()
            candles = await fetch_candles(auth, PAIRS)
            if candles:
                await push_candles(candles)
            else:
                logger.warning("No candles fetched this cycle")
            elapsed = round(time.time() - t0, 1)
            wait = max(5, PUSH_INTERVAL - elapsed)
            logger.info("Cycle done in %.1fs, waiting %.0fs...", elapsed, wait)
            await asyncio.sleep(wait)
        except Exception as e:
            logger.error("Cycle error: %s", e)
            await asyncio.sleep(15)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped.")
