"""
BingX WebSocket Service — live price stream.
No geo-restrictions. Works on Render free tier.
"""

import asyncio
import json
import gzip
import websockets
from utils.safe_float import safe_float

SYMBOLS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT",
    "ADA-USDT", "DOGE-USDT", "AVAX-USDT", "DOT-USDT", "LINK-USDT",
    "UNI-USDT", "LTC-USDT", "ATOM-USDT", "NEAR-USDT", "MATIC-USDT",
    "APT-USDT", "ARB-USDT", "OP-USDT", "SUI-USDT", "PEPE-USDT",
]

WS_URL = "wss://open-api-swap.bingx.com/swap-market"


async def start_bingx_ws(cache_callback):
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                print("BingX WSS connected")

                for sym in SYMBOLS:
                    sub_msg = {
                        "id":     sym,
                        "reqType": "sub",
                        "dataType": f"{sym}@ticker",
                    }
                    await ws.send(json.dumps(sub_msg))
                    await asyncio.sleep(0.05)

                async for message in ws:
                    try:
                        # BingX sends gzip compressed data
                        if isinstance(message, bytes):
                            try:
                                text = gzip.decompress(message).decode("utf-8")
                            except Exception:
                                text = message.decode("utf-8")
                        else:
                            text = message

                        data = json.loads(text)

                        # Handle ping
                        if data.get("ping"):
                            await ws.send(json.dumps({"pong": data["ping"]}))
                            continue

                        ticker = data.get("data", {})
                        if not ticker:
                            continue

                        symbol = data.get("dataType", "").replace("@ticker", "").replace("-", "")
                        price  = safe_float(ticker.get("c", 0))

                        if not symbol or price <= 0:
                            continue

                        open_price = safe_float(ticker.get("o", price))
                        change24h  = ((price - open_price) / open_price * 100) if open_price else 0

                        await cache_callback(
                            symbol    = symbol,
                            price     = price,
                            change24h = round(change24h, 4),
                            volume    = safe_float(ticker.get("v", 0)),
                            high      = safe_float(ticker.get("h", 0)),
                            low       = safe_float(ticker.get("l", 0)),
                            source    = "bingx",
                        )
                    except Exception:
                        continue

        except Exception as e:
            print(f"BingX WSS error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)
