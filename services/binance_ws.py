"""
Binance WebSocket Service — live price stream.
Connects to Binance WSS (not blocked on Render).
Feeds prices directly into market_cache.
"""

import asyncio
import json
import websockets
from utils.safe_float import safe_float

# Top 20 USDT pairs to track
SYMBOLS = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
    "adausdt", "dogeusdt", "avaxusdt", "dotusdt", "linkusdt",
    "uniusdt", "ltcusdt", "atomusdt", "nearusdt", "maticusdt",
    "aptusdt", "arbusdt", "opusdt", "suiusdt", "pepeusdt",
]

STREAM = "/".join([f"{s}@ticker" for s in SYMBOLS])
WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAM}"


async def start_binance_ws(cache_callback):
    """
    Connect to Binance WebSocket and call cache_callback
    with each price update.
    cache_callback(symbol, price, change24h, volume, high, low)
    Reconnects automatically on disconnect.
    """
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                print("Binance WSS connected")
                async for message in ws:
                    try:
                        data   = json.loads(message)
                        ticker = data.get("data", {})
                        if ticker.get("e") != "24hrTicker":
                            continue
                        await cache_callback(
                            symbol    = ticker["s"],
                            price     = safe_float(ticker["c"]),
                            change24h = safe_float(ticker["P"]),
                            volume    = safe_float(ticker["q"]),
                            high      = safe_float(ticker["h"]),
                            low       = safe_float(ticker["l"]),
                            source    = "binance",
                        )
                    except Exception:
                        continue
        except Exception as e:
            print(f"Binance WSS error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)
