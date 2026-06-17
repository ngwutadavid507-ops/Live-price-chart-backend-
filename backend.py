from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
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


@app.get("/symbols")
async def symbols():
    headers = {
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }

    params = {
        "limit": 50,
        "convert": "USD"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(CMC_URL, headers=headers, params=params) as r:
            data = await r.json()

    coins = data.get("data", [])

    return [
        {
            "symbol": c["symbol"] + "USDT",
            "price": c["quote"]["USD"]["price"],
            "change": c["quote"]["USD"]["percent_change_24h"],
            "volume": c["quote"]["USD"]["volume_24h"]
        }
        for c in coins
    ]
