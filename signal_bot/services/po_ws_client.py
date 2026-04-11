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
from services.pocket_browser import WS_AUTH_PATH, WS_AUTH_PATH_MON
from services.pocket_browser import _candles_from_binary_frames

logger = logging.getLogger(__name__)

# How long to wait for candle data after subscribing to a symbol (seconds).
# Increased from 8→12 to allow time for both:
#   1. updateHistoryNewFast (~1-2s after changeSymbol+ps)
#   2. updateHistoryPeriod  (~3-6s after loadHistoryPeriod request)
_HISTORY_TIMEOUT = 12

# Minimum candles required to consider a response valid
_MIN_CANDLES = 10

# If we already have this many candles from updateHistoryNewFast, we still
# wait up to this many additional seconds for updateHistoryPeriod to arrive.
_EXTRA_WAIT_FOR_PERIOD = 5.0

# Live payout cache — populated as a side-effect of every candle fetch.
# {normalised_symbol: payout_int}  e.g. {"eurusd_otc": 82}
# Written by _fetch_symbol(), read by get_live_payouts().
_live_payouts: dict[str, int] = {}
_seen_fetch_binary_events: set[str] = set()  # tracks non-candle event names seen in _fetch_symbol

# Full asset registry — populated from updateAssets binary event at auth handshake.
# {normalised_key: {"name": str, "symbol": str, "payout": int, "category": str}}
# e.g. {"eurusd_otc": {"name": "EUR/USD OTC", "symbol": "EURUSD_otc", "payout": 92, "category": "currency"}}
_live_assets: dict[str, dict] = {}


def _parse_pct_simple(v) -> int:
    """Parse a payout value: int/float ∈ (1,100] or (0,1) → integer percent."""
    if isinstance(v, (int, float)):
        if 1 < v <= 100:
            return int(round(v))
        if 0 < v < 1:
            return int(round(v * 100))
    return 0


def get_live_payouts() -> dict[str, int]:
    """Return the latest known payout dict, populated from WS candle fetches."""
    return dict(_live_payouts)


def get_live_assets() -> dict[str, dict]:
    """
    Return the full asset registry from the last updateAssets handshake event.
    Keys are normalised (e.g. "eurusd_otc"). Values: {name, symbol, payout, category}.
    Empty until the first WS auth completes (~8 sec after startup).
    """
    return dict(_live_assets)


# Default WS endpoint — used when building auth from env var (no file to read url from)
_DEFAULT_WS_URL = (
    "wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket"
)


def parse_ssid_string(ssid: str) -> dict | None:
    """
    Parse a raw SSID string (copied from browser DevTools) into an auth dict.

    Accepted formats:
      1. Full Socket.IO message:
         42["auth",{"session":"...","isDemo":1,"uid":"123"}]
      2. JSON auth object only:
         {"session":"...","isDemo":1,"uid":"123"}
      3. Bare PHP session string:
         a:4:{s:10:"session_id";s:32:"...";...}

    Returns {"session": ..., "isDemo": ..., "uid": ...} or None on failure.
    """
    ssid = ssid.strip().strip('"').strip("'")

    # Format 1 — full Socket.IO message: 42["auth", {...}]
    if ssid.startswith("42"):
        try:
            payload = json.loads(ssid[2:])   # strip leading "42"
            if isinstance(payload, list) and len(payload) >= 2:
                auth = payload[1]
                if isinstance(auth, dict) and (
                    "session" in auth or "sessionToken" in auth
                ):
                    return auth
        except Exception:
            pass

    # Format 2 — bare JSON object
    if ssid.startswith("{"):
        try:
            auth = json.loads(ssid)
            if isinstance(auth, dict) and (
                "session" in auth or "sessionToken" in auth
            ):
                return auth
        except Exception:
            pass

    # Format 3 — bare PHP session string (a:4:{...})
    if ssid.startswith("a:"):
        return {"session": ssid, "isDemo": 1}

    return None


def _parse_php_session(auth_payload: dict) -> dict:
    """
    Extract fields from a PHP-serialised CI session stored in auth_payload["session"].

    Returns a dict with any of: session_id, user_agent, ip_address.
    Returns {} if the session field is missing or not in PHP format.
    """
    import re as _re
    session = auth_payload.get("session", "")
    if not session.startswith("a:"):
        return {}
    result = {}
    for key in ("session_id", "user_agent", "ip_address"):
        pat = rf's:{len(key)}:"{key}";s:\d+:"([^"]+)"'
        m = _re.search(pat, session)
        if m:
            result[key] = m.group(1)
    return result


def _session_cookie_header(auth_payload: dict) -> str:
    """ci_session=<session_id> ready for use as Cookie header value, or ''."""
    fields = _parse_php_session(auth_payload)
    sid = fields.get("session_id", "")
    return f"ci_session={sid}" if sid else ""


def _session_user_agent(auth_payload: dict) -> str:
    """
    Return the User-Agent stored inside the PHP session, or a generic fallback.
    Using the original UA avoids server-side session validation failures (400).
    """
    fields = _parse_php_session(auth_payload)
    return fields.get(
        "user_agent",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    )


def apply_ssid_from_env() -> bool:
    """
    Read PO_SSID env var and, if valid, overwrite po_ws_auth.json so the WS
    client can connect without needing a browser login.

    Also accepts PO_WS_URL to override the default WebSocket endpoint.

    Returns True if the env var was found and parsed successfully.
    Call this once at startup before the candle cache refresher starts.
    """
    ssid_raw = os.environ.get("PO_SSID", "").strip()
    if not ssid_raw:
        return False

    auth = parse_ssid_string(ssid_raw)
    if not auth:
        logger.error(
            "PO_SSID env var is set but could not be parsed — "
            "expected format: 42[\"auth\",{...}] or {\"session\":\"...\",\"uid\":\"...\"}. "
            "WS auth file NOT updated."
        )
        return False

    ws_url = os.environ.get("PO_WS_URL", _DEFAULT_WS_URL).strip()
    data   = {"ws_url": ws_url, "auth": auth}

    try:
        WS_AUTH_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(WS_AUTH_PATH, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(
            "✅ WS auth written from PO_SSID env var → %s (uid=%s)",
            WS_AUTH_PATH, auth.get("uid", "?"),
        )
        return True
    except Exception as e:
        logger.error("Could not write WS auth from PO_SSID: %s", e)
        return False


def load_auth(path: Path | None = None) -> dict | None:
    """Return saved auth dict {ws_url, auth} or None if not yet captured."""
    target = path or WS_AUTH_PATH
    try:
        if Path(target).exists():
            with open(target) as f:
                return json.load(f)
    except Exception as e:
        logger.warning("Could not load WS auth from %s: %s", target, e)
    return None


def load_monitor_auth() -> dict | None:
    """Return auth for the monitoring (secondary) account, falling back to main."""
    auth = load_auth(WS_AUTH_PATH_MON)
    if auth:
        return auth
    logger.debug("Monitor WS auth not found — falling back to main auth")
    return load_auth()


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
        "User-Agent": _session_user_agent(auth_payload),
    }
    cookie = _session_cookie_header(auth_payload)
    if cookie:
        headers["Cookie"] = cookie

    results: dict[str, list[dict]] = {}

    # ── EIO handshake: HTTP polling → get SID → WebSocket upgrade ──────────
    # Socket.IO requires a polling handshake before WebSocket upgrade.
    # Without SID the server returns 400 on the WS upgrade request.
    http_base = (
        ws_url.replace("wss://", "https://").replace("ws://", "http://")
             .split("?")[0]
    )
    try:
        async with aiohttp.ClientSession() as http:
            # Step 1: GET polling handshake → sid
            try:
                r = await asyncio.wait_for(
                    http.get(http_base,
                             params={"EIO": "4", "transport": "polling"},
                             headers=headers),
                    timeout=10.0,
                )
                body = await r.text()
                logger.info("EIO handshake: status=%d body=%s", r.status, body[:120])
            except Exception as e:
                logger.error("fetch_all_pairs: EIO polling handshake failed: %s", e)
                return results

            if r.status != 200 or not body.startswith("0"):
                logger.error(
                    "fetch_all_pairs: unexpected handshake response status=%d body=%s",
                    r.status, body[:200],
                )
                return results

            import re as _re
            sid_m = _re.search(r'"sid"\s*:\s*"([^"]+)"', body)
            if not sid_m:
                logger.error("fetch_all_pairs: no sid in handshake body: %s", body[:200])
                return results
            sid = sid_m.group(1)
            logger.info("EIO handshake: got sid=%s", sid)

            # Step 2: POST "40" (namespace connect)
            await asyncio.wait_for(
                http.post(http_base,
                          params={"EIO": "4", "transport": "polling", "sid": sid},
                          data="40",
                          headers={**headers, "Content-Type": "text/plain"}),
                timeout=5.0,
            )

            # Step 3: drain the connect ACK
            await asyncio.wait_for(
                http.get(http_base,
                         params={"EIO": "4", "transport": "polling", "sid": sid},
                         headers=headers),
                timeout=5.0,
            )

            # Step 4a: try WebSocket upgrade with the established sid
            ws_url_with_sid = f"{ws_url}&sid={sid}"
            ws_ok = False
            try:
                async with http.ws_connect(
                    ws_url_with_sid,
                    headers=headers,
                    heartbeat=None,
                    receive_timeout=None,
                    autoclose=False,
                    autoping=False,
                ) as ws:
                    ws_ok = True
                    queue: asyncio.Queue[Any] = asyncio.Queue()
                    reader_task = asyncio.create_task(_reader(ws, queue))
                    ping_task   = asyncio.create_task(_keepalive(ws, 25.0))
                    try:
                        ping_interval = await _handshake(ws, auth_payload, queue)
                        ping_task.cancel()
                        ping_task = asyncio.create_task(_keepalive(ws, ping_interval))
                        for symbol in symbols:
                            asset = symbol.lstrip("#")
                            candles = await _fetch_symbol(ws, asset, queue)
                            if candles:
                                results[symbol] = candles
                                logger.info("Direct WS: %s → %d candles", symbol, len(candles))
                            else:
                                logger.warning("Direct WS: no candles for %s", symbol)
                    finally:
                        reader_task.cancel()
                        ping_task.cancel()
            except Exception as ws_err:
                if ws_ok:
                    raise  # WS connected but something else failed — propagate
                logger.warning(
                    "WS upgrade failed (%s) — falling back to HTTP polling for candles", ws_err
                )

            # Step 4b: HTTP polling fallback (used when WebSocket upgrade is blocked)
            if not ws_ok and not results:
                results = await _fetch_candles_via_polling(
                    http, http_base, sid, auth_payload, symbols, headers
                )

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error("fetch_all_pairs failed: %s", e)

    return results


async def _fetch_candles_via_polling(
    http: aiohttp.ClientSession,
    http_base: str,
    sid: str,
    auth_payload: dict,
    symbols: list[str],
    headers: dict,
    per_symbol_timeout: float = 15.0,
) -> dict[str, list[dict]]:
    """
    HTTP long-polling fallback for candle data.

    Used when WebSocket upgrade to api-eu.po.market is blocked (returns 400)
    while HTTP polling still works (EIO handshake returns 200).

    Binary event attachments (base64 prefixed with 'b') contain the same
    JSON payload that _candles_from_binary_frames expects.
    """
    import base64 as _b64
    results: dict[str, list[dict]] = {}
    pt     = {"EIO": "4", "transport": "polling", "sid": sid}
    ct_hdr = {**headers, "Content-Type": "text/plain"}

    async def _post(data: str) -> None:
        await asyncio.wait_for(
            http.post(http_base, params=pt, data=data, headers=ct_hdr),
            timeout=5.0,
        )

    async def _poll(t: float = 8.0) -> str:
        r = await asyncio.wait_for(
            http.get(http_base, params=pt, headers=headers),
            timeout=t + 2,
        )
        return await r.text()

    def _parse_packets(text: str) -> list[str]:
        """Split EIO4 polling response into individual packets (separated by \x1e)."""
        if not text:
            return []
        # EIO4: length-prefixed packets OR \x1e-separated packets
        if "\x1e" in text:
            return [p for p in text.split("\x1e") if p]
        # Single packet (possibly length-prefixed: "NNNpacket")
        # Strip leading decimal length prefix if present
        stripped = text.lstrip("0123456789")
        return [stripped] if stripped else []

    # Send auth
    auth_msg = "42" + json.dumps(["auth", auth_payload])
    await _post(auth_msg)

    # Drain auth ACK (a few polls)
    for _ in range(3):
        try:
            ack = await _poll(5.0)
            logger.info("Polling auth ACK raw: %s", ack[:300])
            if "auth" in ack.lower():
                break
        except asyncio.TimeoutError:
            break

    logger.info("HTTP polling fallback: fetching %d symbols", len(symbols))

    for symbol in symbols:
        asset = symbol.lstrip("#")
        now_ts  = int(time.time())
        from_ts = now_ts - 720 * 60  # 720 one-minute bars back

        change_msg  = "42" + json.dumps(["changeSymbol", {"asset": asset, "period": 60}])
        history_msg = "42" + json.dumps([
            "loadHistoryPeriod",
            {"asset": asset, "period": 60, "from": from_ts, "count": 720},
        ])
        await _post(change_msg)
        await _post(history_msg)

        deadline = time.monotonic() + per_symbol_timeout
        candles: list[dict] = []
        pending_binary_event: str | None = None  # event name waiting for binary blob

        while time.monotonic() < deadline and not candles:
            try:
                txt = await _poll(min(8.0, deadline - time.monotonic()))
            except asyncio.TimeoutError:
                break
            except Exception as e:
                logger.warning("Polling error for %s: %s", asset, e)
                break

            logger.info("Polling [%s] raw (%d): %s", asset, len(txt), txt[:300])
            for pkt in _parse_packets(txt):
                # Engine.IO PONG (heartbeat reply) — ignore
                if pkt in ("3", "2"):
                    continue

                # Binary attachment: b<base64>
                if pkt.startswith("b"):
                    try:
                        raw_bytes = _b64.b64decode(pkt[1:] + "==")  # pad for safety
                        ev_name = pending_binary_event or "updateHistoryPeriod"
                        parsed = _candles_from_binary_frames(
                            [(ev_name, raw_bytes)], count=9999,
                            symbol=symbol, period=60,
                        )
                        if parsed:
                            candles = parsed
                            logger.info(
                                "HTTP polling: %s → %d candles (binary/%s)",
                                asset, len(candles), ev_name,
                            )
                    except Exception as e:
                        logger.debug("Binary parse error for %s: %s", asset, e)
                    pending_binary_event = None
                    continue

                # Socket.IO binary event header: 451-[["eventName", {}]]
                if pkt.startswith("451-"):
                    try:
                        header = json.loads(pkt[4:])
                        if isinstance(header, list) and header:
                            pending_binary_event = header[0] if isinstance(header[0], str) else None
                    except Exception:
                        pass
                    continue

                # Regular Socket.IO text event: 42["eventName", data]
                if pkt.startswith("42"):
                    try:
                        payload = json.loads(pkt[2:])
                        if isinstance(payload, list) and len(payload) >= 2:
                            ev  = payload[0]
                            dat = payload[1]
                            if ev in _HISTORY_EVENTS and isinstance(dat, dict):
                                # Candle data came as inline JSON (non-binary path)
                                raw = json.dumps(dat).encode()
                                parsed = _candles_from_binary_frames(
                                    [(ev, raw)], count=9999,
                                    symbol=symbol, period=60,
                                )
                                if parsed:
                                    candles = parsed
                                    logger.info(
                                        "HTTP polling: %s → %d candles (text/%s)",
                                        asset, len(candles), ev,
                                    )
                    except Exception as e:
                        logger.debug("Text event parse error for %s: %s", asset, e)

        if not candles:
            logger.warning("HTTP polling: no candles received for %s", asset)
        else:
            results[symbol] = candles

    logger.info(
        "HTTP polling fallback done: %d/%d pairs got candles",
        len(results), len(symbols),
    )
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

    # Additional messages to request asset/payout data
    for msg in [
        '42["openOptions"]',
        '42["getOptions"]',
        '42["assets"]',
        '42["loadAssets"]',
        '42["instruments"]',
    ]:
        try:
            await ws.send_str(msg)
        except Exception:
            pass

    # Drain server responses for up to 8 sec — log ALL events to discover payout source
    _seen_events: set[str] = set()
    drain_deadline = time.monotonic() + 8.0
    auth_done = False
    while time.monotonic() < drain_deadline:
        remaining = drain_deadline - time.monotonic()
        try:
            item = await asyncio.wait_for(queue.get(), timeout=min(remaining, 1.0))
        except asyncio.TimeoutError:
            if auth_done:
                break  # auth confirmed, no more events coming
            continue
        if item is None:
            raise RuntimeError("Server closed connection during auth")
        if item[0] == "text":
            text = item[1]
            # Log every unique event name at INFO so we can discover what PO sends
            if text.startswith("42"):
                try:
                    parsed = json.loads(text[2:])
                    ev = str(parsed[0]) if isinstance(parsed, list) else ""
                    if ev and ev not in _seen_events:
                        _seen_events.add(ev)
                        logger.info(
                            "WS handshake text event: %r  preview=%s",
                            ev, str(parsed[1] if len(parsed) > 1 else "")[:200],
                        )
                    # Try to capture payout from every event
                    _capture_payout_from_text(text)
                except Exception:
                    pass
            if "updateBalance" in text or "successauth" in text or "updateProfile" in text:
                auth_done = True
        elif item[0] == "binary":
            _, bin_event, bin_raw = item
            if bin_event and bin_event not in _seen_events:
                _seen_events.add(bin_event)
                logger.info(
                    "WS handshake binary event: %r  raw_len=%d  hex_preview=%s",
                    bin_event, len(bin_raw), bin_raw[:32].hex(),
                )
            # Auth confirmed via binary event
            if bin_event in ("successauth", "successupdateBalance", "updateBalance"):
                auth_done = True
            # Parse updateAssets — contains full payout table for all instruments
            if bin_event == "updateAssets" and bin_raw:
                parsed_payouts = _parse_update_assets_binary(bin_raw)
                if parsed_payouts:
                    _live_payouts.update(parsed_payouts)
                    otc_count = sum(1 for k in parsed_payouts if "otc" in k)
                    logger.info(
                        "updateAssets parsed: %d total assets, %d OTC pairs with payouts",
                        len(parsed_payouts), otc_count,
                    )
                    # Log OTC payouts specifically
                    otc_payouts = {k: v for k, v in parsed_payouts.items() if "otc" in k}
                    logger.info("OTC payouts from updateAssets: %s", otc_payouts)
                else:
                    logger.warning(
                        "updateAssets received (%d bytes) but parsed 0 payouts — raw preview: %s",
                        len(bin_raw), bin_raw[:100],
                    )

    payout_count = len(_live_payouts)
    logger.info(
        "Direct WS: authenticated (uid=%s) — handshake drained %d events, %d payouts captured",
        auth_payload.get("uid"), len(_seen_events), payout_count,
    )
    return ping_interval


# ── updateAssets binary parser ─────────────────────────────────────────────────

def _parse_update_assets_binary(raw: bytes) -> dict[str, int]:
    """
    Parse the 'updateAssets' binary WS frame → {normalised_symbol: payout_int}.

    PocketOption sends this during auth handshake.  The payload is UTF-8 JSON array.

    Confirmed schema per entry: [asset_id, "SYMBOL", "Name", "category", type_id, payout_pct, ...]
    - Stock OTC symbols have "#" prefix: "#AAPL", "#MSFT_otc"
    - All other OTC symbols don't: "EURUSD_otc", "AEDCNY_otc", "ADA-USD_otc"
    - Payout is always index 5 as integer percent (e.g. 92 = 92%, 0 = unavailable)
    """
    global _live_assets
    result: dict[str, int] = {}
    assets: dict[str, dict] = {}
    try:
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, list):
            return result
        for entry in data:
            if not isinstance(entry, list) or len(entry) < 6:
                continue
            # Schema: [asset_id, "SYMBOL", "Name", "category", type_id, payout_pct, ...]
            raw_sym  = entry[1]
            name     = entry[2] if len(entry) > 2 and isinstance(entry[2], str) else ""
            category = entry[3] if len(entry) > 3 and isinstance(entry[3], str) else ""
            if not isinstance(raw_sym, str) or not raw_sym:
                continue
            # Normalise key: "EURUSD_otc" / "#AAPL_otc" → "eurusd_otc" / "aapl_otc"
            key = raw_sym.lstrip("#").lower().replace("/", "").replace(" ", "_")
            # Payout at index 5
            v = entry[5]
            if not isinstance(v, (int, float)):
                continue
            if 30 <= v <= 100:
                pct = int(round(v))
            elif 0.30 <= v <= 1.0:
                pct = int(round(v * 100))
            else:
                continue
            result[key] = pct
            # Store full asset info — used by pairs_cache for dynamic pair list
            assets[key] = {
                "name":     name,                         # e.g. "AED/CNY OTC"
                "symbol":   raw_sym.lstrip("#"),          # e.g. "AEDCNY_otc"
                "payout":   pct,
                "category": category,                     # "currency", "stock", etc.
            }
        if assets:
            _live_assets = assets
            otc_currency = sum(
                1 for k, a in assets.items()
                if "_otc" in k and a["category"] == "currency"
            )
            logger.info(
                "updateAssets: %d assets parsed, %d currency OTC pairs",
                len(assets), otc_currency,
            )
    except Exception as e:
        logger.warning("_parse_update_assets_binary failed: %s", e)
    return result


# ── Payout capture (side-effect of candle fetch) ──────────────────────────────

def _capture_payout_from_text(text: str, hint_asset: str = "") -> None:
    """
    Try to extract payout % from any text WS frame and store in _live_payouts.
    Called for every text frame received during _fetch_symbol so that we capture
    payouts as a free side-effect of the candle-fetching loop.
    """
    global _live_payouts
    if not text.startswith("42"):
        return
    try:
        data = json.loads(text[2:])
    except Exception:
        return
    if not isinstance(data, list) or len(data) < 2:
        return

    event_name = str(data[0])
    payload    = data[1]

    # Log every new event so we know exactly what PO sends after changeSymbol
    if not hasattr(_capture_payout_from_text, "_seen"):
        _capture_payout_from_text._seen = set()  # type: ignore[attr-defined]
    seen: set = _capture_payout_from_text._seen   # type: ignore[attr-defined]
    if event_name not in seen:
        seen.add(event_name)
        logger.info(
            "WS text event after changeSymbol: %r  payload_preview=%s",
            event_name, str(payload)[:200],
        )

    # Look for payout in the payload dict
    if not isinstance(payload, dict):
        return

    # Determine which asset this event belongs to
    asset_key = (
        payload.get("asset")
        or payload.get("symbol")
        or payload.get("id")
        or hint_asset
    )
    if not asset_key:
        return
    asset_key = str(asset_key).lstrip("#").lower()
    if not asset_key.endswith("_otc"):
        asset_key += "_otc"

    # Extract payout — try common field names
    for field in ("profit", "payout", "profitability", "return", "percent",
                  "win_rate", "winrate", "value"):
        raw = payload.get(field)
        if raw is None:
            continue
        pct = _parse_pct_simple(raw)
        if pct > 30:
            if _live_payouts.get(asset_key) != pct:
                logger.info("Live payout captured: %s → %d%%  (event=%r field=%r)",
                            asset_key, pct, event_name, field)
            _live_payouts[asset_key] = pct
            return


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

    # Explicitly request a full page of historical candles (100+ bars).
    # PO responds with updateHistoryPeriod binary event (same schema as
    # updateHistoryNewFast). _candles_from_binary_frames now accepts both.
    # time = current unix ms; index=0 = most recent page (~2h of 1m candles).
    hist_req = json.dumps([
        "loadHistoryPeriod",
        {
            "asset":  asset,
            "period": 60,                          # 1-minute candles
            "time":   int(time.time() * 1000),     # current time in ms
            "index":  0,                           # page 0 = most recent
        },
    ])
    await ws.send_str("42" + hist_req)

    deadline = time.monotonic() + _HISTORY_TIMEOUT
    binary_frames: list[tuple[str, bytes]] = []
    got_fast_history = False          # received updateHistoryNewFast
    got_period_history = False        # received updateHistoryPeriod
    fast_history_at: float = 0.0     # when we got updateHistoryNewFast

    from services.pocket_browser import _HISTORY_EVENTS

    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            # Cancelling queue.get() is SAFE — ws.receive() in reader is unaffected
            item = await asyncio.wait_for(queue.get(), timeout=min(remaining, 2.0))
        except asyncio.TimeoutError:
            # If we have fast history and waited long enough for period history, stop early
            if got_fast_history and not got_period_history:
                waited = time.monotonic() - fast_history_at
                if waited >= _EXTRA_WAIT_FOR_PERIOD:
                    logger.debug(
                        "%s: no updateHistoryPeriod after %.1fs — using fast history only",
                        asset, waited,
                    )
                    break
            continue   # check deadline again

        if item is None:
            # Connection closed
            logger.warning("Direct WS: connection closed while fetching %s", asset)
            break

        kind = item[0]
        if kind == "text":
            # Capture payout from text responses to changeSymbol (e.g. updateStream)
            _capture_payout_from_text(item[1], asset)
        elif kind == "binary":
            _, event_name, raw = item

            # Track which history events we receive
            if event_name == "updateHistoryNewFast":
                got_fast_history = True
                fast_history_at = time.monotonic()
            elif event_name == "updateHistoryPeriod":
                got_period_history = True
                logger.info("✓ updateHistoryPeriod received for %s (%d bytes)", asset, len(raw))

            # Log unknown binary events (for debugging PO protocol)
            if event_name and event_name not in _HISTORY_EVENTS:
                if event_name not in _seen_fetch_binary_events:
                    _seen_fetch_binary_events.add(event_name)
                    logger.info(
                        "_fetch_symbol(%s): non-history binary event %r len=%d  preview=%s",
                        asset, event_name, len(raw), raw[:60],
                    )
                    try:
                        obj = json.loads(raw.decode("utf-8"))
                        logger.info("  → decoded as JSON: %s", str(obj)[:300])
                    except Exception:
                        pass

            binary_frames.append((event_name, raw))

            # Stop early only when we have BOTH history sources
            if got_fast_history and got_period_history:
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
    Monitor a pair in real-time for up to max_duration seconds.

    Opens a persistent WS connection, calls on_candles(candles) on every
    check_interval. If the server closes the connection (e.g. because the
    candle-cache refresher switches assets on the same account), automatically
    reconnects and continues until max_duration is reached or on_candles
    returns True.

    symbol format: '#EURUSD_otc'  (with leading #, same as candle_cache keys)
    """
    auth_data = load_monitor_auth()
    if not auth_data:
        logger.warning("stream_pair: WS auth not available")
        return

    asset      = symbol.lstrip("#")
    ws_url     = auth_data["ws_url"]
    auth_payload = auth_data["auth"]
    headers = {
        "Origin":  "https://pocketoption.com",
        "Referer": "https://pocketoption.com/en/cabinet/demo-quick-high-low/",
        "User-Agent": _session_user_agent(auth_payload),
    }
    cookie = _session_cookie_header(auth_payload)
    if cookie:
        headers["Cookie"] = cookie

    start_time = time.monotonic()

    # Outer reconnect loop — keeps running until max_duration or signal found
    while True:
        elapsed = time.monotonic() - start_time
        if elapsed >= max_duration:
            logger.info("stream_pair: max duration reached for %s", asset)
            return

        ws_closed_unexpectedly = False

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

                        # Subscribe to the asset
                        sub_msg = json.dumps(["changeSymbol", {"asset": asset, "period": 60}])
                        await ws.send_str("42" + sub_msg)
                        await ws.send_str('42["ps"]')

                        # Inner loop — collect frames each interval
                        while True:
                            elapsed = time.monotonic() - start_time
                            if elapsed >= max_duration:
                                logger.info("stream_pair: timeout for %s", asset)
                                return

                            collect_deadline = time.monotonic() + check_interval
                            binary_frames: list[tuple[str, bytes]] = []
                            ws_closed_this_window = False

                            while time.monotonic() < collect_deadline:
                                remaining = collect_deadline - time.monotonic()
                                try:
                                    item = await asyncio.wait_for(
                                        queue.get(), timeout=min(remaining, 2.0)
                                    )
                                except asyncio.TimeoutError:
                                    continue

                                if item is None:
                                    logger.warning(
                                        "stream_pair: WS closed by server for %s — will reconnect",
                                        asset,
                                    )
                                    ws_closed_this_window = True
                                    ws_closed_unexpectedly = True
                                    break

                                if item[0] == "binary":
                                    _, event_name, raw = item
                                    binary_frames.append((event_name, raw))

                            if ws_closed_this_window:
                                break  # exit inner loop → reconnect

                            candles = _parse_frames(binary_frames, asset) if binary_frames else None
                            if not candles:
                                await ws.send_str('42["ps"]')
                                logger.debug("stream_pair: no frames for %s, re-pulling", asset)
                                continue

                            logger.debug(
                                "stream_pair: %s — %d candles, checking signal",
                                asset, len(candles),
                            )
                            should_stop = await on_candles(candles)
                            if should_stop:
                                logger.info("stream_pair: done for %s (signal or stop)", asset)
                                return

                            await ws.send_str('42["ps"]')

                    finally:
                        reader_task.cancel()
                        ping_task.cancel()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("stream_pair: connection error for %s: %s — will reconnect", asset, e)
            ws_closed_unexpectedly = True

        if ws_closed_unexpectedly:
            wait = min(5.0, max_duration - (time.monotonic() - start_time))
            if wait > 0:
                logger.info("stream_pair: reconnecting for %s in %.1fs…", asset, wait)
                await asyncio.sleep(wait)
            else:
                return


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


# ── HTTP polling payout fetcher (fallback — no browser needed) ────────────────

async def fetch_payouts_http_polling(timeout: float = 15.0) -> dict[str, int]:
    """
    Use Socket.IO HTTP long-polling transport to fetch payout data.

    Works on the same po.market server as the WS connection — not blocked by
    Cloudflare (different domain from pocketoption.com).

    Returns {normalised_symbol: payout_int} or {} on failure.
    """
    auth_data = load_auth()
    if not auth_data:
        return {}

    ws_url       = auth_data["ws_url"]
    auth_payload = auth_data["auth"]

    # Convert wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket
    # →  https://demo-api-eu.po.market/socket.io/
    http_base = ws_url.replace("wss://", "https://").replace("ws://", "http://")
    http_base = http_base.split("?")[0]  # strip query string

    headers = {
        "Origin":  "https://pocketoption.com",
        "Referer": "https://pocketoption.com/en/cabinet/demo-quick-high-low/",
        "User-Agent": _session_user_agent(auth_payload),
    }
    cookie = _session_cookie_header(auth_payload)
    if cookie:
        headers["Cookie"] = cookie

    payouts: dict[str, int] = {}
    seen_events: set[str]   = set()

    def _scan_for_payouts(obj, depth: int = 0) -> None:
        if depth > 8 or not obj:
            return
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    pct = 0
                    for k in ("profit", "payout", "profitability", "percent", "win_rate"):
                        v = item.get(k)
                        if isinstance(v, (int, float)) and 30 < v <= 100:
                            pct = int(round(v))
                            break
                        if isinstance(v, (int, float)) and 0 < v < 1:
                            pct = int(round(v * 100))
                            break
                    sym = item.get("symbol") or item.get("asset") or item.get("ticker") or item.get("name") or ""
                    if pct > 30 and sym:
                        key = str(sym).lower().replace("#", "").replace("/", "").replace(" ", "").replace("-", "")
                        payouts[key] = pct
                    else:
                        _scan_for_payouts(item, depth + 1)
        elif isinstance(obj, dict):
            for v in obj.values():
                _scan_for_payouts(v, depth + 1)

    try:
        async with aiohttp.ClientSession(headers=headers) as http:
            # Step 1: EIO handshake — get session ID
            resp = await asyncio.wait_for(
                http.get(http_base, params={"EIO": "4", "transport": "polling"}),
                timeout=10.0,
            )
            body = await resp.text()
            logger.info("HTTP polling handshake: status=%d body=%s", resp.status, body[:120])
            if resp.status != 200 or not body.startswith("0"):
                logger.warning("HTTP polling: unexpected handshake response, aborting")
                return {}
            import re as _re
            sid_m = _re.search(r'"sid"\s*:\s*"([^"]+)"', body)
            if not sid_m:
                logger.warning("HTTP polling: no sid in handshake")
                return {}
            sid = sid_m.group(1)
            logger.info("HTTP polling: got sid=%s", sid)

            # Step 2: Send Socket.IO CONNECT
            await asyncio.wait_for(
                http.post(http_base, params={"EIO": "4", "transport": "polling", "sid": sid},
                          data="40", headers={"Content-Type": "text/plain"}),
                timeout=5.0,
            )

            # Step 3: Poll once to drain CONNECT ACK
            resp2 = await asyncio.wait_for(
                http.get(http_base, params={"EIO": "4", "transport": "polling", "sid": sid}),
                timeout=5.0,
            )
            ack = await resp2.text()
            logger.info("HTTP polling CONNECT ACK: %s", ack[:80])

            # Step 4: Send auth
            auth_msg = "42" + json.dumps(["auth", auth_payload])
            await asyncio.wait_for(
                http.post(http_base, params={"EIO": "4", "transport": "polling", "sid": sid},
                          data=auth_msg, headers={"Content-Type": "text/plain"}),
                timeout=5.0,
            )

            # Step 5: Poll for responses up to `timeout` seconds
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    resp3 = await asyncio.wait_for(
                        http.get(http_base, params={"EIO": "4", "transport": "polling", "sid": sid}),
                        timeout=5.0,
                    )
                    text = await resp3.text()
                    if not text:
                        await asyncio.sleep(0.5)
                        continue
                    logger.debug("HTTP polling response: %s", text[:200])
                    # Multiple packets separated by length prefix (EIO4 format): <len>\x1e<data>
                    parts = text.split("\x1e") if "\x1e" in text else [text]
                    for part in parts:
                        # Strip numeric length prefix if present
                        p = part.lstrip("0123456789")
                        if not p.startswith("42"):
                            continue
                        try:
                            parsed = json.loads(p[2:])
                            ev = str(parsed[0]) if isinstance(parsed, list) else ""
                            payload = parsed[1] if len(parsed) > 1 else None
                            if ev and ev not in seen_events:
                                seen_events.add(ev)
                                logger.info(
                                    "HTTP polling event: %r  preview=%s",
                                    ev, str(payload)[:150],
                                )
                            if payload:
                                _scan_for_payouts(payload if isinstance(payload, list) else [payload])
                        except Exception:
                            pass
                    if payouts:
                        logger.info("HTTP polling: captured %d payouts", len(payouts))
                        break
                except asyncio.TimeoutError:
                    break
                except Exception as e:
                    logger.warning("HTTP polling error: %s", e)
                    break

    except Exception as e:
        logger.warning("fetch_payouts_http_polling failed: %s", e)

    logger.info(
        "fetch_payouts_http_polling done — %d events seen, %d payouts found",
        len(seen_events), len(payouts),
    )
    return payouts


# ── Live payout fetcher ────────────────────────────────────────────────────────

async def fetch_asset_payouts(timeout: float = 12.0) -> dict[str, int]:
    """
    Connect to PocketOption WS and collect real-time payout % for all OTC assets.

    Returns {normalised_symbol: payout_int}, e.g. {"eurusd_otc": 82, ...}
    Empty dict if nothing found or auth not available yet.

    Logs every unique event name received — crucial for diagnosing why data is
    missing if the returned dict is empty.
    """
    import re as _re

    auth_data = load_auth()
    if not auth_data:
        logger.warning("fetch_asset_payouts: WS auth not yet available")
        return {}

    ws_url       = auth_data["ws_url"]
    auth_payload = auth_data["auth"]

    headers = {
        "Origin":  "https://pocketoption.com",
        "Referer": "https://pocketoption.com/en/cabinet/demo-quick-high-low/",
        "User-Agent": _session_user_agent(auth_payload),
    }
    cookie = _session_cookie_header(auth_payload)
    if cookie:
        headers["Cookie"] = cookie

    payouts: dict[str, int] = {}
    seen_events: set[str]   = set()

    def _parse_pct(v) -> int:
        if isinstance(v, (int, float)):
            if 1 < v <= 100:
                return int(round(v))
            if 0 < v < 1:
                return int(round(v * 100))
        if isinstance(v, str):
            m = _re.search(r"(\d{2,3})", v)
            if m:
                p = int(m.group(1))
                if 30 < p <= 100:
                    return p
        return 0

    def _scan_obj(obj, depth: int = 0) -> None:
        """Recursively scan any object for asset-with-payout entries."""
        if depth > 8 or not obj:
            return
        if isinstance(obj, list):
            for item in obj:
                _scan_obj(item, depth + 1)
        elif isinstance(obj, dict):
            # Check if this dict looks like an asset entry
            sym = ""
            pct = 0
            for k, v in obj.items():
                kl = k.lower()
                if kl in ("symbol", "asset", "id", "ticker", "code", "name"):
                    if isinstance(v, str) and "_otc" in v.lower():
                        sym = v.lstrip("#")
                if kl in ("profit", "payout", "return", "percent",
                          "profitability", "winrate", "win_rate", "value"):
                    p = _parse_pct(v)
                    if p > 30:
                        pct = p
            if sym and pct:
                key = sym.lower().lstrip("#")
                if not key.endswith("_otc"):
                    key += "_otc"
                if key not in payouts:
                    payouts[key] = pct
                    logger.info("fetch_asset_payouts: %s → %d%%", key, pct)
            # Recurse into values
            for v in obj.values():
                if isinstance(v, (list, dict)):
                    _scan_obj(v, depth + 1)

    def _process_event(event_name: str, payload) -> None:
        if event_name not in seen_events:
            seen_events.add(event_name)
            logger.info(
                "fetch_asset_payouts WS event: %r  payload_type=%s  payload_preview=%s",
                event_name, type(payload).__name__, str(payload)[:120],
            )
        _scan_obj(payload)

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
                queue: asyncio.Queue[Any] = asyncio.Queue()
                reader_task = asyncio.create_task(_reader(ws, queue))
                ping_task   = asyncio.create_task(_keepalive(ws, 25.0))

                try:
                    ping_interval = await _handshake(ws, auth_payload, queue)
                    ping_task.cancel()
                    ping_task = asyncio.create_task(_keepalive(ws, ping_interval))

                    # Send candidate messages that might trigger an asset-list response
                    for msg in [
                        '42["assets"]',
                        '42["getAssets"]',
                        '42["loadAssets"]',
                        '42["updateAssets"]',
                        '42["assets/load"]',
                        '42["instruments"]',
                        '42["loadInstruments"]',
                        '42["openOptions"]',
                    ]:
                        try:
                            await ws.send_str(msg)
                        except Exception:
                            pass

                    # Also subscribe to a few OTC pairs — the changeSymbol response
                    # may include asset metadata / payout
                    import config as _cfg
                    for p in _cfg.OTC_PAIRS[:5]:
                        asset = p["symbol"].lstrip("#")
                        sub = json.dumps(["changeSymbol", {"asset": asset, "period": 60}])
                        try:
                            await ws.send_str("42" + sub)
                        except Exception:
                            pass

                    # Drain ALL text frames for `timeout` seconds
                    deadline = time.monotonic() + timeout
                    while time.monotonic() < deadline:
                        remaining = deadline - time.monotonic()
                        try:
                            item = await asyncio.wait_for(
                                queue.get(), timeout=min(remaining, 1.5)
                            )
                        except asyncio.TimeoutError:
                            continue
                        if item is None:
                            break
                        if item[0] != "text":
                            continue
                        text = item[1]
                        if not text.startswith("42"):
                            continue
                        try:
                            data = json.loads(text[2:])
                        except Exception:
                            continue
                        if not isinstance(data, list) or len(data) < 1:
                            continue
                        event_name = str(data[0])
                        payload    = data[1] if len(data) > 1 else {}
                        _process_event(event_name, payload)

                finally:
                    reader_task.cancel()
                    ping_task.cancel()

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error("fetch_asset_payouts WS error: %s", e)

    logger.info(
        "fetch_asset_payouts done: %d payouts found. All events seen: %s",
        len(payouts), sorted(seen_events),
    )
    return payouts
