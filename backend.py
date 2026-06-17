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
async def root():
    return {"status": "running"}


@app.get("/health")
async def health():
    return {"status": "ok", "ws_route": "/ws"}


@app.get("/symbols")
async def symbols():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BINANCE_URL, timeout=15) as r:

                if r.status != 200:
                    return {
                        "error": f"Binance returned {r.status}"
                    }

                data = await r.json()

                if not isinstance(data, list):
                    return {
                        "error": "Unexpected Binance response",
                        "response": data
                    }

                return [
                    {
                        "symbol": x["symbol"],
                        "price": float(x["lastPrice"]),
                        "change": float(x["priceChangePercent"]),
                        "volume": float(x["volume"])
                    }
                    for x in data
                    if x["symbol"].endswith("USDT")
                ]

    except Exception as e:
        return {"error": str(e)}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):

    await ws.accept()

    while True:

        try:

            async with aiohttp.ClientSession() as session:
                async with session.get(BINANCE_URL, timeout=15) as r:

                    if r.status != 200:
                        await ws.send_json({
                            "type": "error",
                            "message": f"Binance returned {r.status}"
                        })
                        await asyncio.sleep(5)
                        continue

                    data = await r.json()

            if not isinstance(data, list):
                await ws.send_json({
                    "type": "error",
                    "message": "Invalid Binance response"
                })
                await asyncio.sleep(5)
                continue

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

        except Exception as e:

            await ws.send_json({
                "type": "error",
                "message": str(e)
            })

        await asyncio.sleep(2)
