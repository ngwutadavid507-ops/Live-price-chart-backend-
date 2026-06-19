from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import aiohttp
import asyncio
import time
import os
import random
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
CMC_API_KEY = os.getenv("CMC_API_KEY")
COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_CHART = "https://api.coingecko.com/api/v3/coins/{id}/market_chart"

CACHE_TTL = 60
WS_INTERVAL = 5
RATE_LIMIT_MAX = 10
RATE_LIMIT_WINDOW = 60
MAX_WS_CLIENTS = 100

# ═══════════════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════════════
cache = {"all": [], "hot": [], "ready": False, "last_update": 0, "id_map": {}}
rate_limits = defaultdict(list)
ws_clients = set()
http_session = None

# ═══════════════════════════════════════════════════════════════
#  HTTP CLIENT
# ═══════════════════════════════════════════════════════════════
async def fetch_json(url, headers=None, params=None):
    global http_session
    if http_session is None:
        return None
    try:
        async with http_session.get(url, headers=headers, params=params) as r:
            text = await r.text()
            if r.status == 429:
                print(f"[FETCH] RATE LIMITED: {url[:60]}")
                return None
            if r.status != 200:
                print(f"[FETCH] HTTP {r.status}: {text[:100]}")
                return None
            import json
            return json.loads(text)
    except Exception as e:
        print(f"[FETCH] ERROR: {type(e).__name__}: {str(e)[:80]}")
        return None

# ═══════════════════════════════════════════════════════════════
#  COINGECKO
# ═══════════════════════════════════════════════════════════════
async def fetch_coingecko_markets():
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h"
    }
    data = await fetch_json(COINGECKO_MARKETS, params=params)
    if not isinstance(data, list):
        return [], {}
    result = []
    id_map = {}
    for coin in data:
        if not isinstance(coin, dict):
            continue
        symbol = coin.get("symbol", "").upper()
        result.append({
            "symbol": symbol,
            "name": coin.get("name"),
            "price": coin.get("current_price") or 0,
            "change": coin.get("price_change_percentage_24h") or 0,
            "volume": coin.get("total_volume") or 0,
            "market_cap": coin.get("market_cap") or 0,
            "high_24h": coin.get("high_24h") or 0,
            "low_24h": coin.get("low_24h") or 0,
        })
        id_map[symbol] = coin.get("id")
    print(f"[COINGECKO] Markets: {len(result)} coins")
    return result, id_map

# ═══════════════════════════════════════════════════════════════
#  BUILD CACHE
# ═══════════════════════════════════════════════════════════════
async def build_cache():
    global cache
    result, id_map = await fetch_coingecko_markets()
    if not result:
        print("[CACHE] No data from CoinGecko")
        if not cache["all"]:
            cache["ready"] = True
        return
    cache["all"] = result
    cache["hot"] = result[:100]
    cache["id_map"] = id_map
    cache["ready"] = True
    cache["last_update"] = time.time()
    print(f"[CACHE] Built: {len(result)} symbols")

# ═══════════════════════════════════════════════════════════════
#  BACKGROUND
# ═══════════════════════════════════════════════════════════════
async def background_fetcher():
    while True:
        try:
            await build_cache()
        except Exception as e:
            print(f"[BG ERROR] {type(e).__name__}: {e}")
        await asyncio.sleep(CACHE_TTL)

async def ws_broadcaster():
    while True:
        await asyncio.sleep(WS_INTERVAL)
        if not ws_clients or not cache["ready"]:
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

# ═══════════════════════════════════════════════════════════════
#  RATE LIMIT
# ═══════════════════════════════════════════════════════════════
def check_rate_limit(ip):
    now = time.time()
    rate_limits[ip] = [t for t in rate_limits[ip] if now - t < RATE_LIMIT_WINDOW]
    if len(rate_limits[ip]) >= RATE_LIMIT_MAX:
        return False
    rate_limits[ip].append(now)
    return True

# ═══════════════════════════════════════════════════════════════
#  LIFESPAN
# ═══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_session
    http_session = aiohttp.ClientSession()
    print(f"[STARTUP] Ready")
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
    allow_methods=["*"],
    allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════════════
#  ROUTES
# ═══════════════════════════════════════════════════════════════
@app.get("/")
async def home():
    return {"status": "running", "symbols": len(cache["all"]), "source": "coingecko"}

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "cache_ready": cache["ready"],
        "symbols": len(cache["all"]),
        "ws_clients": len(ws_clients)
    }

@app.get("/symbols")
async def symbols(request: Request):
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")
    if not cache["ready"]:
        await build_cache()
    return cache["all"]

@app.get("/candles/{symbol}")
async def candles(symbol: str, request: Request, days: str = "7"):
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")

    symbol_upper = symbol.upper()
    coin_id = cache.get("id_map", {}).get(symbol_upper)

    if not coin_id:
        return generate_demo_candles()

    url = COINGECKO_CHART.format(id=coin_id)
    params = {"vs_currency": "usd", "days": days}

    data = await fetch_json(url, params=params)

    if not data or not isinstance(data, dict):
        return generate_demo_candles()

    prices = data.get("prices", [])
    if not prices or len(prices) < 10:
        return generate_demo_candles()

    # Aggregate into candles based on number of data points
    total_points = len(prices)
    candles_per_agg = max(1, total_points // 100)  # Target ~100 candles

    candles = []
    bucket = []
    bucket_start = None

    for ts, price in prices:
        if bucket_start is None:
            bucket_start = ts

        bucket.append((ts, price))

        if len(bucket) >= candles_per_agg:
            opens = [p for _, p in bucket]
            candles.append({
                "time": bucket_start,
                "open": opens[0],
                "high": max(p for _, p in bucket),
                "low": min(p for _, p in bucket),
                "close": opens[-1]
            })
            bucket_start = None
            bucket = []

    if bucket:
        opens = [p for _, p in bucket]
        candles.append({
            "time": bucket_start,
            "open": opens[0],
            "high": max(p for _, p in bucket),
            "low": min(p for _, p in bucket),
            "close": opens[-1]
        })

    print(f"[CANDLES] {symbol_upper}: {len(candles)} candles from {total_points} points")
    return candles

def generate_demo_candles():
    now = int(time.time() * 1000)
    base_price = 50000 + random.random() * 20000
    candles = []

    for i in range(100):
        ts = now - (100 - i) * 3600 * 1000
        change = (random.random() - 0.48) * 0.02
        open_p = base_price
        close_p = base_price * (1 + change)
        high_p = max(open_p, close_p) * (1 + random.random() * 0.005)
        low_p = min(open_p, close_p) * (1 - random.random() * 0.005)

        candles.append({
            "time": ts,
            "open": round(open_p, 2),
            "high": round(high_p, 2),
            "low": round(low_p, 2),
            "close": round(close_p, 2)
        })
        base_price = close_p

    return candles

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    if len(ws_clients) >= MAX_WS_CLIENTS:
        await ws.close(code=1008)
        return
    await ws.accept()
    ws_clients.add(ws)
    if cache["ready"]:
        await ws.send_json({
            "type": "hot",
            "data": cache["hot"],
            "timestamp": int(time.time() * 1000)
        })
    try:
        while True:
            msg = await asyncio.wait_for(ws.receive_text(), timeout=30)
            if msg == "ping":
                await ws.send_json({"type": "pong"})
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    finally:
        ws_clients.discard(ws)
