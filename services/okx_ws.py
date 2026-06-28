"""
OKX WebSocket Service — live price stream.
No geo-restrictions. Works on Render free tier.
"""

import asyncio
import json
import websockets
from utils.safe_float import safe_float

SYMBOLS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT",
    "ADA-USDT", "DOGE-USDT", "AVAX-USDT", "DOT-USDT", "LINK-USDT",
    "UNI-USDT", "LTC-USDT", "ATOM-USDT", "NEAR-USDT", "MATIC-USDT",
    "APT-USDT", "ARB-USDT", "OP-USDT", "SUI-USDT", "PEPE-USDT",
]

WS_URL = "wss://ws.okx.com:8443/ws/v5/public"


async def start_okx_ws(cache_callback):
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                print("OKX WSS connected")

                sub_msg = {
                    "op": "subscribe",
                    "args": [
                        {"channel": "tickers", "instId": sym}
                        for sym in SYMBOLS
                    ],
                }
                await ws.send(json.dumps(sub_msg))

                async for message in ws:
                    try:
                        data  = json.loads(message)
                        event = data.get("event", "")

                        if event in ("subscribe", "error"):
                            continue

                        items = data.get("data", [])
                        for ticker in items:
                            inst_id = ticker.get("instId", "")
                            symbol  = inst_id.replace("-", "")
                            price   = safe_float(ticker.get("last"))

                            if not symbol or price <= 0:
                                continue

                            open24h = safe_float(ticker.get("open24h", price))
                            change24h = ((price - open24h) / open24h * 100) if open24h else 0

                            await cache_callback(
                                symbol    = symbol,
                                price     = price,
                                change24h = round(change24h, 4),
                                volume    = safe_float(ticker.get("volCcy24h", 0)),
                                high      = safe_float(ticker.get("high24h", 0)),
                                low       = safe_float(ticker.get("low24h", 0)),
                                source    = "okx",
                            )
                    except Exception:
                        continue

        except Exception as e:
            print(f"OKX WSS error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)
