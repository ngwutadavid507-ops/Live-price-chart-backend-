import asyncio
import json
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import websockets

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# GLOBAL STATE
# -----------------------------
clients = set()
latest_data = {}  # symbol -> data

# -----------------------------
# HELPERS
# -----------------------------
def to_list():
    return list(latest_data.values())

# -----------------------------
# BINANCE STREAM
# -----------------------------
async def binance_stream():
    url = "wss://fstream.binance.com/ws/!ticker@arr"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                async for msg in ws:
                    data = json.loads(msg)

                    for t in data:
                        symbol = t["s"]

                        latest_data[symbol] = {
                            "symbol": symbol,
                            "price": float(t["c"]),
                            "change": float(t["P"]),
                            "volume": float(t["v"]),
                            "exchange": "binance",
                            "time": time.time()
                        }

        except Exception:
            await asyncio.sleep(3)

# -----------------------------
# BYBIT STREAM
# -----------------------------
async def bybit_stream():
    url = "wss://stream.bybit.com/v5/public/linear"

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:

                await ws.send(json.dumps({
                    "op": "subscribe",
                    "args": ["tickers.*"]
                }))

                async for msg in ws:
                    data = json.loads(msg)

                    if "data" in data:
                        for t in data["data"]:
                            symbol = t.get("symbol")

                            if not symbol:
                                continue

                            latest_data[symbol] = {
                                "symbol": symbol,
                                "price": float(t.get("lastPrice", 0)),
                                "change": float(t.get("price24hPcnt", 0)) * 100,
                                "volume": float(t.get("volume24h", 0)),
                                "exchange": "bybit",
                                "time": time.time()
                            }

        except Exception:
            await asyncio.sleep(3)

# -----------------------------
# WEBSOCKET CLIENT MANAGER
# -----------------------------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)

    try:
        while True:
            await ws.send_json({
                "type": "prices",
                "data": to_list()
            })
            await asyncio.sleep(1)

    except WebSocketDisconnect:
        clients.discard(ws)

    except Exception:
        clients.discard(ws)

# -----------------------------
# SYMBOLS ENDPOINT (OPTIONAL)
# -----------------------------
@app.get("/symbols")
def symbols():
    return list(latest_data.keys())

# -----------------------------
# STARTUP TASKS
# -----------------------------
@app.on_event("startup")
async def startup():
    asyncio.create_task(binance_stream())
    asyncio.create_task(bybit_stream())
