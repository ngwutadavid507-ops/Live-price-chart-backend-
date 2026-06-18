from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import aiohttp
import asyncio
import time
import os
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
CMC_API_KEY = os.getenv("CMC_API_KEY")
CMC_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
BINANCE_PRICE_URL = "https://api.binance.com/api/v3/ticker/price"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

CACHE_TTL = 5
WS_INTERVAL = 5
RATE_LIMIT_MAX = 10
RATE_LIMIT_WINDOW = 60
MAX_WS_CLIENTS = 100

# ═══════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════
cache = {"all": [], "hot": [], "last_update": 0}
rate_limits = defaultdict(list)
ws_clients = set()
http_session = None


# ═══════════════════════════════════════════════════════════════
# SAFE HTTP
# ═══════════════════════════════════════════════════════════════
async def fetch_json(url, headers=None, params=None):
    global http_session

    try:
        if http_session is None:
            return None

        async with http_session.get(url, headers=headers, params=params) as r:
            return await r.json()

    except Exception as e:
        print(f"[FETCH ERROR] {url}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# CMC
# ═══════════════════════════════════════════════════════════════
async def fetch_cmc(limit=2500):
    if not CMC_API_KEY:
        return []

    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": 1, "limit": limit, "convert": "USD"}

    data = await fetch_json(CMC_URL, headers=headers, params=params)

    if not data or not isinstance(data, dict):
        return []

    return data.get("data", [])


# ═══════════════════════════════════════════════════════════════
# BINANCE
# ═══════════════════════════════════════════════════════════════
async def fetch_binance_prices():
    data = await fetch_json(BINANCE_PRICE_URL)

    if not isinstance(data, list):
        return {}

    mapped = {}

    for item in data:
        if not isinstance(item, dict):
            continue

        symbol = item.get("symbol")
        price = item.get("price")

        if not symbol or not price:
            continue

        if symbol.endswith("USDT"):
            base = symbol.replace("USDT", "")
            try:
                mapped[base] = float(price)
            except:
                continue

    return mapped


# ═══════════════════════════════════════════════════════════════
# MERGE ENGINE
# ═══════════════════════════════════════════════════════════════
def merge_data(cmc_data, binance_data):
    result = []

    for x in cmc_data:
        if not isinstance(x, dict):
            continue

        symbol = x.get("symbol")
        if not symbol:
            continue

        quote = x.get("quote", {}).get("USD", {})

        price = binance_data.get(symbol) or quote.get("price", 0)

        result.append({
            "symbol": symbol,
            "name": x.get("name"),
            "price": float(price),
            "change": quote.get("percent_change_24h", 0),
            "volume": quote.get("volume_24h", 0),
            "source": "binance" if symbol in binance_data else "cmc"
        })

    return result


# ═══════════════════════════════════════════════════════════════
# BACKGROUND LOOP (CRASH SAFE)
# ═══════════════════════════════════════════════════════════════
async def background_fetcher():
    while True:
        try:
            cmc_data, binance_data = await asyncio.gather(
                fetch_cmc(2500),
                fetch_binance_prices()
            )

            merged = merge_data(cmc_data, binance_data)

            cache["all"] = merged
            cache["hot"] = merged[:100]
            cache["last_update"] = time.time()

        except Exception as e:
            print("[BACKGROUND ERROR]", e)

        await asyncio.sleep(CACHE_TTL)


# ═══════════════════════════════════════════════════════════════
# WS BROADCASTER
# ═══════════════════════════════════════════════════════════════
async def ws_broadcaster():
    while True:
        try:
            await asyncio.sleep(WS_INTERVAL)

            if not ws_clients:
                continue

            payload = {
                "type": "hot",
                "data": cache["hot"],
                "timestamp": int(time.time() * 1000)
            }

            dead = set()

            for ws in ws_clients:
                try:
                    await ws.send_json(payload)
                except:
                    dead.add(ws)

            for ws in dead:
                ws_clients.discard(ws)

        except Exception as e:
            print("[WS ERROR]", e)


# ═══════════════════════════════════════════════════════════════
# RATE LIMIT
# ═══════════════════════════════════════════════════════════════
def check_rate_limit(ip):
    now = time.time()

    rate_limits[ip] = [
        t for t in rate_limits[ip]
        if now - t < RATE_LIMIT_WINDOW
    ]

    if len(rate_limits[ip]) >= RATE_LIMIT_MAX:
        return False

    rate_limits[ip].append(now)
    return True


# ═══════════════════════════════════════════════════════════════
# LIFESPAN
# ═══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_session

    http_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=15)
    )

    tasks = [
        asyncio.create_task(background_fetcher()),
        asyncio.create_task(ws_broadcaster())
    ]

    yield

    for t in tasks:
        t.cancel()

    await http_session.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════
@app.get("/")
async def home():
    return {
        "status": "running",
        "engine": "cmc + binance unified FIXED",
        "cache_size": len(cache["all"])
    }


@app.get("/health")
async def health():
    binance_ok = await fetch_json("https://api.binance.com/api/v3/ping")

    return {
        "status": "ok",
        "cmc_key_loaded": bool(CMC_API_KEY),
        "binance_ok": binance_ok is not None,
        "cache_age": int(time.time() - cache["last_update"]) if cache["last_update"] else None,
        "ws_clients": len(ws_clients)
    }


@app.get("/symbols")
async def symbols(request: Request):
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit exceeded")

    if not cache["all"]:
        raise HTTPException(503, "Data not ready yet")

    return cache["all"]


@app.get("/candles/{symbol}")
async def candles(symbol: str, request: Request):
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit exceeded")

    pair = f"{symbol.upper()}USDT"
    url = f"{BINANCE_KLINES_URL}?symbol={pair}&interval=1m&limit=50"

    data = await fetch_json(url)

    if not isinstance(data, list):
        raise HTTPException(502, "Invalid Binance response")

    result = []

    for c in data:
        if not isinstance(c, list) or len(c) < 6:
            continue

        result.append({
            "time": c[0],
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5])
        })

    return result


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):

    if len(ws_clients) >= MAX_WS_CLIENTS:
        await ws.close()
        return

    await ws.accept()
    ws_clients.add(ws)

    try:
        await ws.send_json({
            "type": "hot",
            "data": cache["hot"],
            "timestamp": int(time.time() * 1000)
        })

        while True:
            msg = await asyncio.wait_for(ws.receive_text(), timeout=30)
            if msg == "ping":
                await ws.send_json({"type": "pong"})

    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass

    finally:
        ws_clients.discard(ws)
