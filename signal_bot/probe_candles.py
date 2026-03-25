"""
Diagnostic: connect to PO WS and probe how much tick history is returned
for different changeSymbol.period values.

Run from signal_bot/ directory:
    python probe_candles.py

Uses existing WS auth (ws_auth.json must be present).
"""

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import aiohttp

from services.pocket_browser import _ticks_to_candles
from services.po_ws_client import (
    _handshake,
    _keepalive,
    _reader,
    load_auth,
)

TEST_ASSET   = "EURUSD_otc"
TEST_PERIODS = [5, 15, 60, 300, 1800]   # seconds — sent in changeSymbol
TIMEOUT_SEC  = 12.0                      # how long to wait for history per period


async def probe_period(ws, queue: asyncio.Queue, period: int) -> dict:
    """Subscribe with a given period and capture the raw tick response."""

    sub_msg = json.dumps(["changeSymbol", {"asset": TEST_ASSET, "period": period}])
    await ws.send_str("42" + sub_msg)
    await ws.send_str('42["ps"]')

    deadline = time.monotonic() + TIMEOUT_SEC
    binary_frames: list[tuple[str, bytes]] = []
    all_ticks: list = []

    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        try:
            item = await asyncio.wait_for(queue.get(), timeout=min(remaining, 2.0))
        except asyncio.TimeoutError:
            if all_ticks:
                break
            continue

        if item is None:
            break

        kind = item[0]
        if kind != "binary":
            continue

        _, event_name, raw = item
        if event_name != "updateHistoryNewFast":
            continue

        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception:
            continue

        history = parsed.get("history", [])
        if not isinstance(history, list) or not history:
            continue

        asset = parsed.get("asset", "")
        if asset.upper() != TEST_ASSET.upper():
            continue

        all_ticks.extend(history)
        break   # one frame is enough — PO sends full history in one shot

    return {"period_sent": period, "raw_ticks": all_ticks}


def analyse(result: dict) -> None:
    period_sent = result["period_sent"]
    ticks = result["raw_ticks"]

    if not ticks:
        print(f"\n  period={period_sent:>5}: ❌  no data received")
        return

    # Extract timestamps
    ts_values = []
    for t in ticks:
        if isinstance(t, (list, tuple)) and len(t) >= 2:
            try:
                ts_values.append(float(t[0]))
            except Exception:
                pass

    if not ts_values:
        print(f"\n  period={period_sent:>5}: ❌  ticks have no timestamps")
        return

    ts_min = min(ts_values)
    ts_max = max(ts_values)
    span_seconds = ts_max - ts_min
    span_minutes = span_seconds / 60
    span_hours   = span_minutes / 60

    dt_from = datetime.fromtimestamp(ts_min, tz=timezone.utc).strftime("%H:%M:%S")
    dt_to   = datetime.fromtimestamp(ts_max, tz=timezone.utc).strftime("%H:%M:%S")

    candles_15s  = _ticks_to_candles(ticks, period=15)
    candles_1m   = _ticks_to_candles(ticks, period=60)
    candles_5m   = _ticks_to_candles(ticks, period=300)

    print(
        f"\n  changeSymbol.period = {period_sent:>5}"
        f"\n    Raw ticks  : {len(ticks):>6}"
        f"\n    Time range : {dt_from} → {dt_to} UTC"
        f"\n    Span       : {span_minutes:>6.1f} min  ({span_hours:.2f} hours)"
        f"\n    Candles 15s: {len(candles_15s):>4} bars"
        f"\n    Candles  1m: {len(candles_1m):>4} bars"
        f"\n    Candles  5m: {len(candles_5m):>4} bars"
    )


async def main() -> None:
    auth_data = load_auth()
    if not auth_data:
        print("ERROR: ws_auth.json not found — run the bot first to capture auth")
        return

    ws_url       = auth_data["ws_url"]
    auth_payload = auth_data["auth"]

    headers = {
        "Origin":   "https://pocketoption.com",
        "Referer":  "https://pocketoption.com/en/cabinet/demo-quick-high-low/",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    print(f"Probing PO WS  asset={TEST_ASSET}  periods={TEST_PERIODS}")
    print("=" * 60)

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

                for period in TEST_PERIODS:
                    result = await probe_period(ws, queue, period)
                    analyse(result)
                    await asyncio.sleep(1.0)   # small pause between requests

            finally:
                reader_task.cancel()
                ping_task.cancel()

    print("\n" + "=" * 60)
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
