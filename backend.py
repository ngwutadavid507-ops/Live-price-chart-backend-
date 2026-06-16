import asyncio
import json
import time
import aiohttp
import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

app = FastAPI()

# =========================
# CORE STATE ENGINE
# =========================

prices = {}
candles = {}
orderbook = {}
trades = {}
funding = {}
liquidations = {}

clients = set()
subscriptions = {}

# =========================
# BINANCE PRICE + CANDLES + TRADES
# =========================

async def binance_stream():
    url = "wss://fstream.binance.com/ws/!miniTicker@arr"

    async with websockets.connect(url) as ws:
        while True:
            data = json.loads(await ws.recv())

            for item in data:
                symbol = item["s"]

                prices[symbol] = {
                    "price": float(item["c"]),
                    "volume": float(item["v"]),
                    "time": time.time(),
                    "exchange": "binance"
                }

# =========================
# BYBIT STREAM (PRICE + FUNDING)
# =========================

async def bybit_stream():
    url = "wss://stream.bybit.com/v5/public/linear"

    async with websockets.connect(url) as ws:

        await ws.send(json.dumps({
            "op": "subscribe",
            "args": ["tickers.*", "orderbook.50.*", "publicTrade.*"]
        }))

        while True:
            msg = json.loads(await ws.recv())

            if "data" in msg:
                topic = msg.get("topic", "")

                for item in msg["data"]:

                    symbol = item.get("symbol")

                    # PRICE
                    if "tickers" in topic:
                        prices[symbol] = {
                            "price": float(item["lastPrice"]),
                            "volume": float(item["volume24h"]),
                            "funding": float(item.get("fundingRate", 0)),
                            "time": time.time(),
                            "exchange": "bybit"
                        }

                    # ORDERBOOK
                    elif "orderbook" in topic:
                        orderbook[symbol] = item

                    # TRADES
                    elif "publicTrade" in topic:
                        trades.setdefault(symbol, []).append({
                            "price": item["price"],
                            "size": item["size"],
                            "side": item["side"],
                            "time": item["time"]
                        })

# =========================
# CANDLE ENGINE (1m / 5m / 1h)
# =========================

def update_candle(symbol, price, tf="1m"):
    bucket = int(time.time() // 60)

    key = f"{symbol}_{tf}_{bucket}"

    if key not in candles:
        candles[key] = {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "time": bucket
        }

    c = candles[key]
    c["high"] = max(c["high"], price)
    c["low"] = min(c["low"], price)
    c["close"] = price

# =========================
# LIQUIDATION SIMULATOR (API-BASED EXTENSION SLOT)
# =========================

async def liquidation_engine():
    while True:
        await asyncio.sleep(3)

        # placeholder for real liquidation feeds (Bybit/Binance futures liquidation stream)
        for symbol, p in prices.items():
            if float(p["price"]) % 100 == 0:
                liquidations.setdefault(symbol, []).append({
                    "price": p["price"],
                    "size": 1000,
                    "time": time.time()
                })

# =========================
# BROADCAST ENGINE (LOW LATENCY)
# =========================

async def broadcaster():
    while True:
        await asyncio.sleep(0.5)

        dead = []

        for ws in list(clients):
            try:
                symbol = subscriptions.get(ws)

                if symbol and symbol in prices:

                    await ws.send_json({
                        "type": "symbol",
                        "data": {
                            "price": prices[symbol],
                            "orderbook": orderbook.get(symbol),
                            "trades": trades.get(symbol, [])[-20:],
                            "funding": prices[symbol].get("funding", 0),
                            "liquidations": liquidations.get(symbol, [])
                        }
                    })
                else:
                    await ws.send_json({
                        "type": "hot",
                        "data": sorted(
                            prices.values(),
                            key=lambda x: x.get("volume", 0),
                            reverse=True
                        )[:10]
                    })

            except:
                dead.append(ws)

        for d in dead:
            clients.discard(d)
            subscriptions.pop(d, None)

# =========================
# API + WS
# =========================

@app.get("/")
def home():
    return HTMLResponse(open("index.html").read())

@app.websocket("/ws")
async def ws(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    subscriptions[ws] = None

    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)

            if data["type"] == "subscribe":
                subscriptions[ws] = data["symbol"]

    except:
        clients.discard(ws)
        subscriptions.pop(ws, None)

# =========================
# STARTUP
# =========================

@app.on_event("startup")
async def start():
    asyncio.create_task(binance_stream())
    asyncio.create_task(bybit_stream())
    asyncio.create_task(liquidation_engine())
    asyncio.create_task(broadcaster())
