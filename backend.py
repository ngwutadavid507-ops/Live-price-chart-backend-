from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import aiohttp
import asyncio
import time
import os
from collections import defaultdict

CMC_API_KEY = os.getenv("CMC_API_KEY")

CMC_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
BINANCE_PRICE_URL = "https://api.binance.com/api/v3/ticker/price"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

CACHE_TTL = 5
WS_INTERVAL = 5
RATE_LIMIT_MAX = 10
RATE_LIMIT_WINDOW = 60

cache = {"all": [], "hot": [], "last_update": 0}
rate_limits = defaultdict(list)
ws_clients = set()
http_session = None


# ---------------- HTTP CLIENT ----------------
async def fetch_json(url, headers=None, params=None):
    try:
        async with http_session.get(url, headers=headers, params=params) as r:
            return await r.json()
    except:
        return None


# ---------------- CMC ----------------
async def fetch_cmc(limit=2500):
    if not CMC_API_KEY:
        return []

    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": 1, "limit": limit, "convert": "USD"}

    data = await fetch_json(CMC_URL, headers=headers, params=params)
    return data.get("data", []) if isinstance(data, dict) else []


# ---------------- BINANCE ----------------
async def fetch_binance():
    data = await fetch_json(BINANCE_PRICE_URL)

    if not isinstance(data, list):
        return {}

    out = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol")
        price = item.get("price")
        if symbol and symbol.endswith("USDT"):
            base = symbol.replace("USDT", "")
            try:
                out[base] = float(price)
            except:
                pass
    return out


# ---------------- MERGE ----------------
def merge(cmc, binance):
    result = []

    for x in cmc:
        if not isinstance(x, dict):
            continue

        symbol = x.get("symbol")
        if not symbol:
            continue

        quote = x.get("quote", {}).get("USD", {})

        price = binance.get(symbol) or quote.get("price") or 0

        result.append({
            "symbol": symbol,
            "name": x.get("name", ""),
            "price": float(price),
            "change": quote.get("percent_change_24h", 0),
            "volume": quote.get("volume_24h", 0),
        })

    return result


# ---------------- BACKGROUND LOOP ----------------
async def updater():
    while True:
        cmc, binance = await asyncio.gather(
            fetch_cmc(2500),
            fetch_binance()
        )

        merged = merge(cmc, binance)

        cache["all"] = merged
        cache["hot"] = merged[:100]
        cache["last_update"] = time.time()

        await asyncio.sleep(CACHE_TTL)


# ---------------- WS BROADCAST ----------------
async def broadcaster():
    while True:
        await asyncio.sleep(WS_INTERVAL)

        if not ws_clients:
            continue

        payload = {
            "type": "hot",
            "data": cache["hot"]
        }

        dead = set()
        for ws in ws_clients:
            try:
                await ws.send_json(payload)
            except:
                dead.add(ws)

        for ws in dead:
            ws_clients.discard(ws)


# ---------------- RATE LIMIT ----------------
def check_rate(ip):
    now = time.time()
    rate_limits[ip] = [t for t in rate_limits[ip] if now - t < RATE_LIMIT_WINDOW]

    if len(rate_limits[ip]) >= RATE_LIMIT_MAX:
        return False

    rate_limits[ip].append(now)
    return True


# ---------------- APP LIFESPAN ----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_session
    http_session = aiohttp.ClientSession()

    task1 = asyncio.create_task(updater())
    task2 = asyncio.create_task(broadcaster())

    yield

    task1.cancel()
    task2.cancel()
    await http_session.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------- ROUTES ----------------
@app.get("/")
async def home():
    return {"status": "ok"}


@app.get("/symbols")
async def symbols(request: Request):
    if not check_rate(request.client.host):
        raise HTTPException(429, "Rate limit")

    # 🔥 SAFE RESPONSE ALWAYS
    return cache["all"] or []


@app.get("/candles/{symbol}")
async def candles(symbol: str):
    url = f"{BINANCE_KLINES_URL}?symbol={symbol.upper()}USDT&interval=1m&limit=50"

    data = await fetch_json(url)

    if not isinstance(data, list):
        return []

    return [
        {
            "time": c[0],
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
        }
        for c in data if isinstance(c, list)
    ]


@app.websocket("/ws")
async def ws(ws):
    await ws.accept()
    ws_clients.add(ws)

    try:
        await ws.send_json({"type": "hot", "data": cache["hot"]})

        while True:
            await ws.receive_text()

    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(ws)
