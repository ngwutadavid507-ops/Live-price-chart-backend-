from fastapi import FastAPI, WebSocket
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
# HOME
# -------------------------
@app.get("/")
async def home():
    return {"status": "running"}


@app.get("/health")
async def health():
    return {"status": "ok", "ws_route": "/ws"}


# -------------------------
# SYMBOLS (CMC FIXED)
# -------------------------
@app.get("/symbols")
async def symbols():
    headers = {
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }

    params = {
        "start": 1,
        "limit": 50,
        "convert": "USD"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(CMC_URL, headers=headers, params=params) as r:
            data = await r.json()

    results = []

    for x in data["data"]:
        quote = x["quote"]["USD"]

        results.append({
            "symbol": x["symbol"] + "USDT",
            "price": quote["price"],
            "change": quote["percent_change_24h"],
            "volume": quote["volume_24h"]
        })

    return results


# -------------------------
# WEBSOCKET (CMC STREAM)
# -------------------------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    headers = {
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }

    while True:
        async with aiohttp.ClientSession() as session:
            async with session.get(CMC_URL, headers=headers, params={
                "start": 1,
                "limit": 20,
                "convert": "USD"
            }) as r:
                data = await r.json()

        top = []

        for x in data["data"]:
            q = x["quote"]["USD"]
            top.append({
                "symbol": x["symbol"] + "USDT",
                "price": q["price"],
                "change": q["percent_change_24h"]
            })

        await ws.send_json({
            "type": "hot",
            "data": top,
            "time": time.time()
        })

        await asyncio.sleep(2)
