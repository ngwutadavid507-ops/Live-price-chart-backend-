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

# CoinGecko — free tier: 10-30 calls/minute
COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_OHLC = "https://api.coingecko.com/api/v3/coins/{id}/ohlc"

CACHE_TTL = 30  # Slow down to avoid CoinGecko rate limits
WS_INTERVAL = 10
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
        async with http_session.get(url, headers=headers, params=params, timeout=30) as r:
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
#  COINGECKO MARKETS
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
        print(f"[COINGECKO] Bad response: {type(data)}")
        return []
    
    result = []
    id_map = {}  # symbol -> id mapping for OHLC
    
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
        "source": "coingecko"
    }

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
async def candles(symbol: str, request: Request):
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")
    
    symbol_upper = symbol.upper()
    
    # Get CoinGecko ID from cache
    coin_id = cache.get("id_map", {}).get(symbol_upper)
    
    if not coin_id:
        print(f"[CANDLES] No ID found for {symbol_upper}")
        return []
    
    # CoinGecko OHLC: days=1 for 1-day hourly, or use /market_chart for minute data
    # We'll use market_chart with minute granularity (last ~2 hours)
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "usd",
        "days": "1",  # 1 day of data
    }
    
    data = await fetch_json(url, params=params)
    
    if not data or not isinstance(data, dict):
        return []
    
    # prices: [[timestamp, price], ...]
    prices = data.get("prices", [])
    if not prices or len(prices) < 2:
        return []
    
    # Convert price points to candles (aggregate into ~10-min intervals)
    candles = []
    interval_ms = 10 * 60 * 1000  # 10 minutes
    
    current_bucket = []
    bucket_start = None
    
    for ts, price in prices:
        if bucket_start is None:
            bucket_start = ts
        
        if ts - bucket_start >= interval_ms:
            # Finalize previous bucket
            if current_bucket:
                opens = [p for _, p in current_bucket]
                highs = [p for _, p in current_bucket]
                lows = [p for _, p in current_bucket]
                candles.append({
                    "time": bucket_start,
                    "open": opens[0],
                    "high": max(highs),
                    "low": min(lows),
                    "close": opens[-1]
                })
            bucket_start = ts
            current_bucket = []
        
        current_bucket.append((ts, price))
    
    # Don't forget last bucket
    if current_bucket:
        opens = [p for _, p in current_bucket]
        highs = [p for _, p in current_bucket]
        lows = [p for _, p in current_bucket]
        candles.append({
            "time": bucket_start,
            "open": opens[0],
            "high": max(highs),
            "low": min(lows),
            "close": opens[-1]
        })
    
    print(f"[CANDLES] {symbol_upper}: {len(candles)} candles from {len(prices)} price points")
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
                                
