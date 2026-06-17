from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
import asyncio
import os
import time

app = FastAPI()  # MUST be above ALL routes

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    CMC_API_KEY = os.getenv("CMC_API_KEY")

    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

    headers = {
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }

    while True:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params={"limit": 50, "convert": "USD"}) as r:
                data = await r.json()

        coins = data.get("data", [])

        top = sorted(
            coins,
            key=lambda x: x["quote"]["USD"]["percent_change_24h"],
            reverse=True
        )[:20]

        await ws.send_json({
            "type": "hot",
            "data": [
                {
                    "symbol": c["symbol"] + "USDT",
                    "price": c["quote"]["USD"]["price"],
                    "change": c["quote"]["USD"]["percent_change_24h"],
                    "volume": c["quote"]["USD"]["volume_24h"]
                }
                for c in top
            ],
            "time": time.time()
        })

        await asyncio.sleep(2)
