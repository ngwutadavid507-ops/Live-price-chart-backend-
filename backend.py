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


@app.get("/")
async def home():
    return {"status": "running"}


@app.get("/health")
async def health():
    return {"status": "ok", "ws_route": "/ws"}


# -------------------------
# SYMBOLS (REST API)
# -------------------------
@app.get("/symbols")
async def symbols():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BINANCE_URL) as r:

                return {
                    "status_code": r.status,
                    "preview": await r.text()
                }

    except Exception as e:
        return {
            "error": str(e)
        }


# -------------------------
# WEBSOCKET
# -------------------------
@app.websocket("/ws")
async def ws(ws: WebSocket):
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
