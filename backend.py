import asyncio
import json
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import aiohttp

app = FastAPI()

# -----------------------------
# CORS (ALLOW FRONTEND)
# -----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# STATE
# -----------------------------
clients = set()

# -----------------------------
# BINANCE REST FETCH (STABLE)
# -----------------------------
BINANCE_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"

async def fetch_binance():
    async with aiohttp.ClientSession() as session:
        async with session.get(BINANCE_URL) as resp:
            return await resp.json()

def format_data(raw):
    return [
        {
            "symbol": item["symbol"],
            "price": float(item["lastPrice"]),
            "change": float(item["priceChangePercent"]),
            "volume": float(item["volume"]),
            "exchange": "binance",
            "time": time.time()
        }
        for item in raw[:150]
    ]

# -----------------------------
# WEBSOCKET ENDPOINT
# -----------------------------
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)

    try:
        while True:
            try:
                raw = await fetch_binance()
                data = format_data(raw)

                await ws.send_json({
                    "type": "prices",
                    "data": data
                })

            except Exception as e:
                await ws.send_json({
                    "type": "error",
                    "message": str(e)
                })

            await asyncio.sleep(2)

    except WebSocketDisconnect:
        clients.discard(ws)

# -----------------------------
# SYMBOLS ENDPOINT (WORKING NOW)
# -----------------------------
@app.get("/symbols")
async def symbols():
    try:
        raw = await fetch_binance()
        return [item["symbol"] for item in raw[:200]]
    except:
        return []
