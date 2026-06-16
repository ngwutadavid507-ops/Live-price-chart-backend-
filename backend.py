import asyncio
import json
import time
from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse
import os
import websockets

app = FastAPI()

# =========================
# GLOBAL STATE
# =========================

prices = {}
hot_tokens = []

clients = set()
subscriptions = {}

# =========================
# BINANCE STREAM (ALL FUTURES)
# =========================

async def binance_stream():
    url = "wss://fstream.binance.com/ws/!ticker@arr"

    async with websockets.connect(url) as ws:
        while True:
            try:
                msg = json.loads(await ws.recv())

                for item in msg:
                    symbol = item["s"]

                    prices[symbol] = {
                        "symbol": symbol,
                        "price": float(item["c"]),
                        "change": float(item["P"]),
                        "volume": float(item["v"]),
                        "exchange": "binance",
                        "time": time.time()
                    }

            except Exception as e:
                print("Binance error:", e)
                await asyncio.sleep(2)

# =========================
# BYBIT STREAM (LINEAR)
# =========================

async def bybit_stream():
    url = "wss://stream.bybit.com/v5/public/linear"

    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": ["tickers.*"]
        }))

        while True:
            try:
                msg = json.loads(await ws.recv())

                if "data" in msg:
                    for item in msg["data"]:
                        symbol = item["symbol"]

                        prices[symbol] = {
                            "symbol": symbol,
                            "price": float(item["lastPrice"]),
                            "change": float(item["price24hPcnt"]) * 100,
                            "volume": float(item["volume24h"]),
                            "exchange": "bybit",
                            "time": time.time()
                        }

            except Exception as e:
                print("Bybit error:", e)
                await asyncio.sleep(2)

# =========================
# HOT TOKENS ENGINE
# =========================

async def hot_engine():
    global hot_tokens

    while True:
        await asyncio.sleep(3)

        hot_tokens = sorted(
            prices.values(),
            key=lambda x: x.get("volume", 0),
            reverse=True
        )[:10]

# =========================
# BROADCAST ENGINE
# =========================

async def broadcaster():
    while True:
        await asyncio.sleep(1)

        dead = []

        for ws in list(clients):
            try:
                symbol = subscriptions.get(ws)

                if symbol and symbol in prices:
                    await ws.send_json({
                        "type": "symbol",
                        "data": prices[symbol]
                    })
                else:
                    await ws.send_json({
                        "type": "hot",
                        "data": hot_tokens
                    })

            except:
                dead.append(ws)

        for d in dead:
            clients.discard(d)
            subscriptions.pop(d, None)

# =========================
# FRONTEND ROUTE
# =========================

@app.get("/")
def home():
    path = os.path.join(os.path.dirname(__file__), "index.html")
    return FileResponse(path)

# =========================
# WEBSOCKET API
# =========================

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    subscriptions[ws] = None

    try:
        while True:
            msg = await ws.receive_text()

            try:
                data = json.loads(msg)

                if data.get("type") == "subscribe":
                    subscriptions[ws] = data["symbol"]

            except:
                pass

    except:
        clients.discard(ws)
        subscriptions.pop(ws, None)

# =========================
# STARTUP TASKS
# =========================

@app.on_event("startup")
async def startup():
    asyncio.create_task(binance_stream())
    asyncio.create_task(bybit_stream())
    asyncio.create_task(hot_engine())
    asyncio.create_task(broadcaster())
