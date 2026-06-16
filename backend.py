import asyncio
import json
import time
import websockets
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# STATE
# -----------------------
prices = {}
clients = set()

# -----------------------
# BINANCE STREAM
# -----------------------
async def binance():
    url = "wss://fstream.binance.com/ws/!ticker@arr"

    async with websockets.connect(url) as ws:
        while True:
            data = json.loads(await ws.recv())

            for t in data:
                symbol = t["s"]

                prices[symbol] = {
                    "symbol": symbol,
                    "price": float(t["c"]),
                    "change": float(t["P"]),
                    "volume": float(t["v"]),
                    "exchange": "binance",
                    "time": time.time()
                }

# -----------------------
# BYBIT STREAM
# -----------------------
async def bybit():
    url = "wss://stream.bybit.com/v5/public/linear"

    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": ["tickers.*"]
        }))

        while True:
            msg = json.loads(await ws.recv())

            if "data" in msg:
                for t in msg["data"]:
                    symbol = t["symbol"]

                    prices[symbol] = {
                        "symbol": symbol,
                        "price": float(t["lastPrice"]),
                        "change": float(t["price24hPcnt"]) * 100,
                        "volume": float(t["volume24h"]),
                        "exchange": "bybit",
                        "time": time.time()
                    }

# -----------------------
# BROADCAST LOOP
# -----------------------
async def broadcast():
    while True:
        await asyncio.sleep(1)

        dead = []

        for ws in list(clients):
            try:
                await ws.send_json({
                    "type": "prices",
                    "data": list(prices.values())
                })
            except:
                dead.append(ws)

        for d in dead:
            clients.discard(d)

# -----------------------
# STARTUP
# -----------------------
@app.on_event("startup")
async def start():
    asyncio.create_task(binance())
    asyncio.create_task(bybit())
    asyncio.create_task(broadcast())

# -----------------------
# WEBSOCKET
# -----------------------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)

    try:
        while True:
            await ws.receive_text()
    except:
        clients.discard(ws)
