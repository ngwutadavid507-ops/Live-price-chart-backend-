from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import aiohttp
import asyncio
import time
import os
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
CMC_API_KEY = os.getenv("CMC_API_KEY")

# CoinGecko — free, no key, works from Render
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

# CryptoCompare — free, no key, works from Render  
CRYPTOCOMPARE_HISTO_URL = "https://min-api.cryptocompare.com/data/v2/histominute"

CACHE_TTL = 10
WS_INTERVAL = 10
RATE_LIMIT_MAX = 10
RATE_LIMIT_WINDOW = 60
MAX_WS_CLIENTS = 100

# ═══════════════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════════════
cache = {"all": [], "hot": [], "ready": False, "last_update": 0}
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
        async with http_session.get(url, headers=headers, params=params, timeout=30) as r:
            text = await r.text()
            if r.status != 200:
                print(f"[FETCH] HTTP {r.status}: {text[:100]}")
                return None
            import json
            return json.loads(text)
    except Exception as e:
        print(f"[FETCH] ERROR: {type(e).__name__}: {str(e)[:80]}")
        return None

# ═══════════════════════════════════════════════════════════════
#  COINGECKO (Primary — free, no key, works from Render)
# ═══════════════════════════════════════════════════════════════
async def fetch_coingecko():
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h"
    }

    data = await fetch_json(COINGECKO_URL, params=params)

    if not isinstance(data, list):
        print(f"[COINGECKO] Bad response type: {type(data)}")
        return []

    result = []
    for coin in data:
        if not isinstance(coin, dict):
            continue
        result.append({
            "symbol": coin.get("symbol", "").upper(),
            "name": coin.get("name"),
            "price": coin.get("current_price") or 0,
            "change": coin.get("price_change_percentage_24h") or 0,
            "volume": coin.get("total_volume") or 0,
        })

    print(f"[COINGECKO] Fetched {len(result)} coins")
    return result

# ═══════════════════════════════════════════════════════════════
#  CMC (Fallback only)
# ═══════════════════════════════════════════════════════════════
async def fetch_cmc(limit=2500):
    if not CMC_API_KEY:
        return []
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": 1, "limit": limit, "convert": "USD"}
    data = await fetch_json("https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
                            headers=headers, params=params)
    if not data or "data" not in data:
        return []
    return [
        {
            "symbol": x.get("symbol"),
            "name": x.get("name"),
            "price": x.get("quote", {}).get("USD", {}).get("price", 0),
            "change": x.get("quote", {}).get("USD", {}).get("percent_change_24h", 0),
            "volume": x.get("quote", {}).get("USD", {}).get("volume_24h", 0),
        }
        for x in data["data"] if isinstance(x, dict)
    ]

# ═══════════════════════════════════════════════════════════════
#  BUILD CACHE
# ═══════════════════════════════════════════════════════════════
async def build_cache():
    global cache

    # Try CoinGecko first
    result = await fetch_coingecko()

    # Fallback to CMC
    if not result and CMC_API_KEY:
        print("[CACHE] Falling back to CMC...")
        result = await fetch_cmc(2500)

    if not result:
        print("[CACHE] WARNING: No data available")
        if not cache["all"]:
            cache["ready"] = True  # Mark ready even if empty
        return

    cache["all"] = result
    cache["hot"] = result[:100]
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
    print(f"[STARTUP] Ready. CMC key: {bool(CMC_API_KEY)}")
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
    return {
        "status": "running",
        "symbols": len(cache["all"]),
        "source": "coingecko",
        "cmc_key_loaded": bool(CMC_API_KEY)
    }

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "cache_ready": cache["ready"],
        "symbols": len(cache["all"]),
        "ws_clients": len(ws_clients),
        "cmc_key_loaded": bool(CMC_API_KEY)
    }

@app.get("/symbols")
async def symbols(request: Request):
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")
    if not cache["ready"]:
        await build_cache()
    return cache["all"]

@app.get("/candles/{symbol}")
async def candles(symbol: str, request: Request):
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")

    # CryptoCompare for candles (works from Render)
    url = f"{CRYPTOCOMPARE_HISTO_URL}?fsym={symbol.upper()}&tsym=USD&limit=50&aggregate=1"
    data = await fetch_json(url)

    if not data or not isinstance(data, dict):
        return []

    histo_data = data.get("Data", {}).get("Data", [])
    if not isinstance(histo_data, list):
        return []

    return [
        {
            "time": c.get("time", 0) * 1000,
            "open": c.get("open", 0),
            "high": c.get("high", 0),
            "low": c.get("low", 0),
            "close": c.get("close", 0),
        }
        for c in histo_data if isinstance(c, dict)
    ]

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
