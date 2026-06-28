"""
Bybit WebSocket Service — live price stream.
Works on Render free tier. No geo-restrictions.
"""

import asyncio
import json
import websockets
from utils.safe_float import safe_float

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
    "UNIUSDT", "LTCUSDT", "ATOMUSDT", "NEARUSDT", "MATICUSDT",
    "APTUSDT", "ARBUSDT", "OPUSDT", "SUIUSDT", "PEPEUSDT",
]

WS_URL = "wss://stream.bybit.com/v5/public/linear"


async def start_bybit_ws(cache_callback):
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                print("Bybit WSS connected")
                sub_msg = {
                    "op":   "subscribe",
                    "args": [f"tickers.{sym}" for sym in SYMBOLS],
                }
                await ws.send(json.dumps(sub_msg))
                async for message in ws:
                    try:
                        data  = json.loads(message)
                        topic = data.get("topic", "")
                        if not topic.startswith("tickers."):
                            continue
                        ticker = data.get("data", {})
                        symbol = ticker.get("symbol", "")
                        price  = safe_float(ticker.get("lastPrice"))
                        if not symbol or price <= 0:
                            continue
                        pct = safe_float(ticker.get("price24hPcnt", 0))
                        if abs(pct) < 1:
                            pct = pct * 100
                        await cache_callback(
                            symbol    = symbol,
                            price     = price,
                            change24h = round(pct, 4),
                            volume    = safe_float(ticker.get("turnover24h", 0)),
                            high      = safe_float(ticker.get("highPrice24h", 0)),
                            low       = safe_float(ticker.get("lowPrice24h", 0)),
                            source    = "bybit",
                        )
                    except Exception:
                        continue
        except Exception as e:
            print(f"Bybit WSS error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)
