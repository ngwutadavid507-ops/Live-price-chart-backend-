import asyncio
import json
import time
import aiohttp
import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

app = FastAPI()

# =========================
# GLOBAL MARKET STORAGE
# =========================

market = {}
hot_tokens = []

clients = set()

# =========================
# SYMBOL DISCOVERY
# =========================

async def get_binance_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            data = await r.json()

    return [
        s["symbol"]
        for s in data["symbols"]
        if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
    ]


async def get_bybit_symbols():
    url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            data = await r.json()

    return [
        s["symbol"]
        for s in data["result"]["list"]
        if s["status"] == "Trading"
    ]

# =========================
# BINANCE STREAM
# =========================

async def binance_ws():
    url = "wss://fstream.binance.com/ws/!ticker@arr"

    async with websockets.connect(url) as ws:
        while True:
            msg = json.loads(await ws.recv())

            for item in msg:
                symbol = item["s"]

                market[symbol] = {
                    "symbol": symbol,
                    "price": float(item["c"]),
                    "change": float(item["P"]),
                    "volume": float(item["v"]),
                    "exchange": "binance",
                    "time": time.time()
                }

# =========================
# BYBIT STREAM (ALL TICKERS)
# =========================

async def bybit_ws():
    url = "wss://stream.bybit.com/v5/public/linear"

    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": ["tickers.*"]
        }))

        while True:
            msg = json.loads(await ws.recv())

            if "data" in msg:
                for item in msg["data"]:
                    symbol = item["symbol"]

                    market[symbol] = {
                        "symbol": symbol,
                        "price": float(item["lastPrice"]),
                        "change": float(item["price24hPcnt"]) * 100,
                        "volume": float(item["volume24h"]),
                        "exchange": "bybit",
                        "time": time.time()
                    }

# =========================
# HOT TOKENS ENGINE
# =========================

async def hot_tokens_engine():
    global hot_tokens

    while True:
        await asyncio.sleep(5)

        sorted_tokens = sorted(
            market.values(),
            key=lambda x: x.get("volume", 0),
            reverse=True
        )

        hot_tokens = sorted_tokens[:10]

# =========================
# WEBSOCKET BROADCAST
# =========================

async def broadcaster():
    while True:
        await asyncio.sleep(1)

        data = {
            "type": "update",
            "market": list(market.values()),
            "hot": hot_tokens
        }

        dead = []

        for ws in clients:
            try:
                await ws.send_json(data)
            except:
                dead.append(ws)

        for d in dead:
            clients.remove(d)

# =========================
# API ROUTES
# =========================

@app.get("/")
def home():
    return HTMLResponse(open("index.html", "r", encoding="utf-8").read())

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)

    try:
        while True:
            await ws.receive_text()
    except:
        clients.remove(ws)

# =========================
# STARTUP
# =========================

@app.on_event("startup")
async def startup():
    asyncio.create_task(binance_ws())
    asyncio.create_task(bybit_ws())
    asyncio.create_task(hot_tokens_engine())
    asyncio.create_task(broadcaster())
