from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
import asyncio
import time
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CMC_API_KEY = os.getenv("CMC_API_KEY")
CMC_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"


# -------------------------
# DEBUG SAFE FETCHER
# -------------------------
async def fetch_cmc(limit=100):
    if not CMC_API_KEY:
        print("CMC ERROR: Missing API key")
        return []

    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": 1, "limit": limit, "convert": "USD"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(CMC_URL, headers=headers, params=params, timeout=15) as r:
                data = await r.json()

        # 🔥 IMPORTANT DEBUG CHECK
        if "data" not in data:
            print("CMC RESPONSE ERROR:", data)
            return []

        return data["data"]

    except Exception as e:
        print("CMC EXCEPTION:", e)
        return []


# -------------------------
# ROOT
# -------------------------
@app.get("/")
async def home():
    return {"status": "running"}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "ws": "/ws",
        "cmc_key_loaded": bool(CMC_API_KEY)
    }


# -------------------------
# SYMBOLS (FULL MARKET SNAPSHOT)
# -------------------------
@app.get("/symbols")
async def symbols():
    data = await fetch_cmc(5000)  # 🔥 max safe attempt

    results = []

    for x in data:
        quote = x.get("quote", {}).get("USD", {})

        results.append({
            "symbol": x.get("symbol"),
            "name": x.get("name"),
            "price": quote.get("price", 0),
            "change": quote.get("percent_change_24h", 0),
            "volume": quote.get("volume_24h", 0),
        })

    return results


# -------------------------
# WEB SOCKET (TOP MOVERS ONLY)
# -------------------------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    try:
        while True:
            data = await fetch_cmc(100)

            top = []
            for x in data:
                q = x.get("quote", {}).get("USD", {})

                top.append({
                    "symbol": x.get("symbol"),
                    "price": q.get("price", 0),
                    "change": q.get("percent_change_24h", 0),
                })

            await ws.send_json({
                "type": "hot",
                "data": top,
                "time": time.time()
            })

            await asyncio.sleep(5)

    except WebSocketDisconnect:
        print("Client disconnected")
