"""
Direct Pocket Option WebSocket Client
======================================
Connects to wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket
using Socket.IO v4 protocol — NO BROWSER, pure Python via aiohttp.

Auth credentials are captured automatically from the browser's first WS
handshake and saved to po_ws_auth.json. Once that file exists, this client
is used instead of the browser for all candle fetching.

Protocol summary (Socket.IO v4 over raw WebSocket):
  Server→Client text:   "0{...}"           = EIO OPEN  (contains pingInterval)
  Client→Server text:   "40"               = Socket.IO CONNECT
  Server→Client text:   "40{...}"          = Socket.IO CONNECT ACK
  Client→Server text:   '42["auth", {...}]'= authenticate
  Client→Server text:   '42["changeSymbol",{"asset":"EURUSD_otc","period":60}]'
  Client→Server text:   '42["ps"]'         = pull stream (triggers history send)
  Server→Client text:   "451-[[\"updateHistoryNewFast\",{}]]"  = binary event header
  Server→Client binary: <raw payload>      = history data (next frame after 451-)
  Client↔Server:        "2" / "3"          = EIO PING / PONG (keepalive)

KEY: Never cancel ws.receive() — use a background reader that feeds an asyncio.Queue.
Cancelling ws.receive() closes the aiohttp transport. Only cancel queue.get() instead.
"""

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import aiohttp

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from services.pocket_browser import WS_AUTH_PATH
from services.pocket_browser import _candles_from_binary_frames

logger = logging.getLogger(__name__)

# How long to wait for candle data after subscribing to a symbol (seconds)
_HISTORY_TIMEOUT = 8

# Minimum candles required to consider a response valid
_MIN_CANDLES = 10


def load_auth() -> dict | None:
    """Return saved auth dict {ws_url, auth} or None if not yet captured."""
    try:
        if Path(WS_AUTH_PATH).exists():
            with open(WS_AUTH_PATH) as f:
                return json.load(f)
    except Exception as e:
        logger.warning("Could not load WS auth: %s", e)
    return None


def is_available() -> bool:
    """True once the browser has captured and saved the auth credentials."""
    return Path(WS_AUTH_PATH).exists()


async def fetch_all_pairs(symbols: list[str]) -> dict[str, list[dict]]:
    """
    Connect once, cycle through all symbols, return {symbol: candles}.
    Much faster than browser (one WS connection, no page rendering).
    Typically completes in 5-10 seconds for 10 pairs.
    """
    auth_data = load_auth()
    if not auth_data:
        logger.warning("WS auth not available yet — browser hasn't captured it")
        return {}

    ws_url = auth_data["ws_url"]
    auth_payload = auth_data["auth"]

    headers = {
        "Origin":  "https://pocketoption.com",
        "Referer": "https://pocketoption.com/en/cabinet/demo-quick-high-low/",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        ),
    }

    results: dict[str, list[dict]] = {}

    try:
        async with aiohttp.ClientSession() as http:
            async with http.ws_connect(
                ws_url,
                headers=headers,
                heartbeat=None,       # manual ping/pong below
                receive_timeout=None, # no per-receive timeout; we manage via queue
                autoclose=False,
                autoping=False,
            ) as ws:
                # message queue — reader task fills it, fetch_symbol drains it
                queue: asyncio.Queue[Any] = asyncio.Queue()

                reader_task  = asyncio.create_task(_reader(ws, queue))
                ping_task    = asyncio.create_task(_keepalive(ws, 25.0))  # default

                try:
                    ping_interval = await _handshake(ws, auth_payload, queue)
                    # update keepalive with the actual server interval
                    ping_task.cancel()
                    ping_task = asyncio.create_task(_keepalive(ws, ping_interval))

                    for symbol in symbols:
                        asset = symbol.lstrip("#")
                        candles = await _fetch_symbol(ws, asset, queue)
                        if candles:
                            results[symbol] = candles
                            logger.info(
                                "Direct WS: %s → %d candles", symbol, len(candles)
                            )
                        else:
                            logger.warning("Direct WS: no candles for %s", symbol)
                finally:
                    reader_task.cancel()
                    ping_task.cancel()

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error("fetch_all_pairs failed: %s", e)

    return results


# ── Reader task ───────────────────────────────────────────────────────────────

async def _reader(ws: aiohttp.ClientWebSocketResponse,
                  queue: asyncio.Queue) -> None:
    """
    Background task: permanently calls ws.receive() and puts raw messages
    onto the queue. NEVER cancelled mid-receive so the transport stays open.
    Puts a sentinel None when the connection closes.
    """
    pending_binary_event: str | None = None
    try:
        while True:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                text = msg.data
                if text == "2":
                    # Server ping → we reply immediately here to be safe
                    try:
                        await ws.send_str("3")
                    except Exception:
                        pass
                elif text.startswith("451-"):
                    # Binary event header
                    try:
                        payload = json.loads(text[4:])
                        pending_binary_event = payload[0] if payload else None
                    except Exception:
                        pending_binary_event = None
                else:
                    await queue.put(("text", text))
            elif msg.type == aiohttp.WSMsgType.BINARY:
                if pending_binary_event:
                    await queue.put(("binary", pending_binary_event, msg.data))
                    pending_binary_event = None
            elif msg.type in (
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.ERROR,
                aiohttp.WSMsgType.CLOSED,
            ):
                logger.warning("Direct WS: reader got close/error: %s", msg.type)
                await queue.put(None)   # sentinel
                return
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error("Direct WS: reader error: %s", e)
        await queue.put(None)   # sentinel


# ── Handshake ─────────────────────────────────────────────────────────────────

async def _handshake(
    ws: aiohttp.ClientWebSocketResponse,
    auth_payload: dict,
    queue: asyncio.Queue,
) -> float:
    """
    Perform Socket.IO handshake and authenticate.
    Returns the server's pingInterval in seconds.
    The reader task is already running and feeding the queue.
    """
    ping_interval = 25.0

    # Receive EIO OPEN from queue (the reader puts text frames there)
    try:
        item = await asyncio.wait_for(queue.get(), timeout=5.0)
        if item and item[0] == "text" and item[1].startswith("0"):
            try:
                info = json.loads(item[1][1:])
                ping_interval = info.get("pingInterval", 25000) / 1000
            except Exception:
                pass
    except asyncio.TimeoutError:
        logger.warning("Direct WS: timed out waiting for EIO OPEN")

    # Send Socket.IO CONNECT
    await ws.send_str("40")

    # Wait for CONNECT ACK
    try:
        ack = await asyncio.wait_for(queue.get(), timeout=5.0)
        if ack and ack[0] == "text" and ack[1].startswith("40"):
            pass  # good
    except asyncio.TimeoutError:
        logger.warning("Direct WS: timed out waiting for CONNECT ACK")

    # Authenticate — request full candle history by disabling fast/optimized modes
    full_auth = {
        **auth_payload,
        "isFastHistory": False,
        "isOptimized":   False,
    }
    await ws.send_str("42" + json.dumps(["auth", full_auth]))

    # Init messages browser sends after auth
    await ws.send_str('42["indicator/load"]')
    await ws.send_str('42["favorite/load"]')
    await ws.send_str('42["price-alert/load"]')

    # Drain server responses for up to 3 sec; stop on updateBalance/updateProfile
    drain_deadline = time.monotonic() + 3.0
    while time.monotonic() < drain_deadline:
        remaining = drain_deadline - time.monotonic()
        try:
            item = await asyncio.wait_for(queue.get(), timeout=remaining)
        except asyncio.TimeoutError:
            break
        if item is None:
            raise RuntimeError("Server closed connection during auth")
        if item[0] == "text":
            text = item[1]
            if "updateBalance" in text or "successauth" in text or "updateProfile" in text:
                logger.debug("Direct WS: auth ack: %s…", text[:80])
                break

    logger.info("Direct WS: authenticated (uid=%s)", auth_payload.get("uid"))
    return ping_interval


# ── Per-symbol fetch ──────────────────────────────────────────────────────────

async def _fetch_symbol(
    ws: aiohttp.ClientWebSocketResponse,
    asset: str,
    queue: asyncio.Queue,
) -> list[dict]:
    """
    Subscribe to `asset` and collect candles from updateHistoryNewFast.
    Safe: we only cancel queue.get(), never ws.receive().
    `asset` format: 'EURUSD_otc' (no leading #).
    """
    sub_msg = json.dumps(["changeSymbol", {"asset": asset, "period": 60}])
    await ws.send_str("42" + sub_msg)
    await ws.send_str('42["ps"]')

    deadline = time.monotonic() + _HISTORY_TIMEOUT
    binary_frames: list[tuple[str, bytes]] = []

    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            # Cancelling queue.get() is SAFE — ws.receive() in reader is unaffected
            item = await asyncio.wait_for(queue.get(), timeout=min(remaining, 2.0))
        except asyncio.TimeoutError:
            continue   # check deadline again

        if item is None:
            # Connection closed
            logger.warning("Direct WS: connection closed while fetching %s", asset)
            break

        kind = item[0]
        if kind == "binary":
            _, event_name, raw = item
            binary_frames.append((event_name, raw))
            candles = _parse_frames(binary_frames, asset)
            if len(candles) >= _MIN_CANDLES:
                return candles

    return _parse_frames(binary_frames, asset)


# ── Real-time streaming monitor ───────────────────────────────────────────────

async def stream_pair(
    symbol: str,
    on_candles,          # async (candles: list[dict]) -> bool  (True = stop)
    max_duration: float = 300.0,
    check_interval: float = 15.0,
) -> None:
    """
    Open one persistent WS connection and call on_candles(candles) every
    check_interval seconds until on_candles returns True or max_duration elapsed.

    Uses the same connect→auth→changeSymbol→ps flow as fetch_all_pairs, but
    keeps the connection alive and re-sends 'ps' each interval instead of
    opening a new connection per check.

    symbol format: '#EURUSD_otc'  (with leading #, same as candle_cache keys)
    """
    auth_data = load_auth()
    if not auth_data:
        logger.warning("stream_pair: WS auth not available")
        return

    asset = symbol.lstrip("#")
    ws_url = auth_data["ws_url"]
    auth_payload = auth_data["auth"]

    headers = {
        "Origin":  "https://pocketoption.com",
        "Referer": "https://pocketoption.com/en/cabinet/demo-quick-high-low/",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        ),
    }

    start_time = time.monotonic()

    try:
        async with aiohttp.ClientSession() as http:
            async with http.ws_connect(
                ws_url,
                headers=headers,
                heartbeat=None,
                receive_timeout=None,
                autoclose=False,
                autoping=False,
            ) as ws:
                queue: asyncio.Queue = asyncio.Queue()
                reader_task = asyncio.create_task(_reader(ws, queue))
                ping_task   = asyncio.create_task(_keepalive(ws, 25.0))

                try:
                    ping_interval = await _handshake(ws, auth_payload, queue)
                    ping_task.cancel()
                    ping_task = asyncio.create_task(_keepalive(ws, ping_interval))

                    logger.info("stream_pair: connected for %s", asset)

                    # Initial subscribe
                    sub_msg = json.dumps(["changeSymbol", {"asset": asset, "period": 60}])
                    await ws.send_str("42" + sub_msg)
                    await ws.send_str('42["ps"]')

                    while True:
                        elapsed = time.monotonic() - start_time
                        if elapsed >= max_duration:
                            logger.info("stream_pair: timeout reached for %s", asset)
                            break

                        # Collect binary frames for up to check_interval seconds
                        collect_deadline = time.monotonic() + check_interval
                        binary_frames: list[tuple[str, bytes]] = []

                        while time.monotonic() < collect_deadline:
                            remaining = collect_deadline - time.monotonic()
                            try:
                                item = await asyncio.wait_for(
                                    queue.get(), timeout=min(remaining, 2.0)
                                )
                            except asyncio.TimeoutError:
                                continue

                            if item is None:
                                logger.warning("stream_pair: WS closed for %s", asset)
                                return

                            if item[0] == "binary":
                                _, event_name, raw = item
                                binary_frames.append((event_name, raw))

                        # Parse whatever we collected
                        candles = _parse_frames(binary_frames, asset) if binary_frames else None

                        if not candles:
                            # No new data in this window — re-pull
                            await ws.send_str('42["ps"]')
                            logger.debug("stream_pair: no frames for %s, re-pulling", asset)
                            continue

                        logger.debug(
                            "stream_pair: %s got %d candles, running signal check",
                            asset, len(candles),
                        )

                        should_stop = await on_candles(candles)
                        if should_stop:
                            logger.info("stream_pair: signal found for %s, stopping", asset)
                            break

                        # Pull next batch
                        await ws.send_str('42["ps"]')

                finally:
                    reader_task.cancel()
                    ping_task.cancel()

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error("stream_pair failed for %s: %s", asset, e)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_frames(binary_frames: list[tuple[str, bytes]], asset: str) -> list[dict]:
    """Reuse the existing binary frame parser from pocket_browser.
    Uses period=15 (15-sec candles, same as browser) so timestamps align for cache merge."""
    if not binary_frames:
        return []
    try:
        return _candles_from_binary_frames(
            binary_frames,
            count=1000,
            symbol=f"#{asset}",
            period=15,            # 15-sec candles — matches browser; gives ~48-60 bars vs ~12 with period=60
        )
    except Exception as e:
        logger.warning("Binary frame parsing failed for %s: %s", asset, e)
        return []


async def _keepalive(ws: aiohttp.ClientWebSocketResponse, interval: float) -> None:
    """Send EIO PING every `interval` seconds to keep the connection alive."""
    try:
        while True:
            await asyncio.sleep(interval * 0.8)
            await ws.send_str("2")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.debug("Keepalive ended: %s", e)
