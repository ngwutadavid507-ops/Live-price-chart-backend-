from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
import asyncio
import time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BINANCE_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"


# -------------------------
# REST: SYMBOL LIST
# -------------------------
@app.get("/symbols")
async def symbols():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://fapi.binance.com/fapi/v1/ticker/24hr",
                timeout=10
            ) as r:

                data = await r.json()

                return [
                    {
                        "symbol": item["symbol"],
                        "price": float(item["lastPrice"]),
                        "change": float(item["priceChangePercent"]),
                        "volume": float(item["volume"])
                    }
                    for item in data
                    if item["symbol"].endswith("USDT")
                ]

    except Exception as e:
        return {
            "error": str(e)
        }


# -------------------------
# WEBSOCKET: LIVE STREAM
# -------------------------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    while True:
        async with aiohttp.ClientSession() as session:
            async with session.get(BINANCE_URL) as r:
                data = await r.json()

        top = sorted(
            data,
            key=lambda x: float(x["priceChangePercent"]),
            reverse=True
        )[:20]

        await ws.send_json({
            "type": "hot",
            "data": [
                {
                    "symbol": x["symbol"],
                    "price": float(x["lastPrice"]),
                    "change": float(x["priceChangePercent"])
                }
                for x in top
            ],
            "time": time.time()
        })

        await asyncio.sleep(2)
