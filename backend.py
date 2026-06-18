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
BINANCE_PRICE_URL = "https://api.binance.com/api/v3/ticker/price"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"


# ---------------------------
# FETCH CMC (GLOBAL TOKENS)
# ---------------------------
async def fetch_cmc(limit=200):
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": 1, "limit": limit, "convert": "USD"}

    async with aiohttp.ClientSession() as session:
        async with session.get(CMC_URL, headers=headers, params=params) as r:
            data = await r.json()

    return data.get("data", [])


# ---------------------------
# FETCH BINANCE PRICES
# ---------------------------
async def fetch_binance_prices():
    async with aiohttp.ClientSession() as session:
        async with session.get(BINANCE_PRICE_URL) as r:
            data = await r.json()

    # map BTCUSDT → BTC
    mapped = {}
    for item in data:
        symbol = item["symbol"]
        if symbol.endswith("USDT"):
            base = symbol.replace("USDT", "")
            mapped[base] = float(item["price"])

    return mapped


# ---------------------------
# UNIFIED SYMBOLS
# ---------------------------
@app.get("/symbols")
async def symbols():
    cmc_data, binance_data = await asyncio.gather(
        fetch_cmc(300),
        fetch_binance_prices()
    )

    result = []

    for x in cmc_data:
        symbol = x["symbol"]
        quote = x["quote"]["USD"]

        binance_price = binance_data.get(symbol)

        result.append({
            "symbol": symbol,
            "name": x["name"],
            "price": binance_price if binance_price else quote["price"],
            "change": quote.get("percent_change_24h", 0),
            "volume": quote.get("volume_24h", 0),
            "source": "binance" if binance_price else "cmc"
        })

    return result


# ---------------------------
# CANDLESTICKS (BINANCE REAL DATA)
# ---------------------------
@app.get("/candles/{symbol}")
async def candles(symbol: str):
    pair = f"{symbol}USDT"

    url = f"{BINANCE_KLINES_URL}?symbol={pair}&interval=1m&limit=50"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            data = await r.json()

    candles = []

    for c in data:
        candles.append({
            "time": c[0],
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5])
        })

    return candles


# ---------------------------
# HEALTH
# ---------------------------
@app.get("/")
async def home():
    return {"status": "running", "engine": "unified cmc + binance"}
@app.get("/price/{symbol}")
async def price(symbol: str):
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as r:
            return await r.json()
