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
CMC_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
BINANCE_PRICE_URL = "https://api.binance.com/api/v3/ticker/price"
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

CACHE_TTL = 5
WS_INTERVAL = 5
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
    try:
        async with http_session.get(url, headers=headers, params=params) as r:
            return await r.json()
    except Exception as e:
        print(f"[FETCH ERROR] {url}: {e}")
        return None

# ═══════════════════════════════════════════════════════════════
#  BINANCE
# ═══════════════════════════════════════════════════════════════
async def fetch_binance():
    data = await fetch_json(BINANCE_PRICE_URL)
    if not isinstance(data, list):
        return {}
    out = {}
    for i in data:
        if not isinstance(i, dict):
            continue
        sym = i.get("symbol")
        price = i.get("price")
        if sym and sym.endswith("USDT") and price:
            try:
                out[sym.replace("USDT", "")] = float(price)
            except:
                continue
    return out

# ═══════════════════════════════════════════════════════════════
#  CMC
# ═══════════════════════════════════════════════════════════════
async def fetch_cmc(limit=2500):
    if not CMC_API_KEY:
        print("[CMC] No API key")
        return []
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": 1, "limit": limit, "convert": "USD"}
    data = await fetch_json(CMC_URL, headers=headers, params=params)
    if not data or "data" not in data:
        return []
    return data["data"]

# ═══════════════════════════════════════════════════════════════
#  BUILD CACHE
# ═══════════════════════════════════════════════════════════════
async def build_cache():
    global cache
    cmc, binance = await asyncio.gather(fetch_cmc(2500), fetch_binance())
    result = []
    for x in cmc:
        if not isinstance(x, dict):
            continue
        symbol = x.get("symbol")
        quote = x.get("quote", {}).get("USD", {})
        price = binance.get(symbol) or quote.get("price") or 0
        result.append({
            "symbol": symbol,
            "name": x.get("name"),
            "price": float(price),
            "change": quote.get("percent_change_24h", 0),
            "volume": quote.get("volume_24h", 0),
        })
    if not result:
        for k, v in binance.items():
            result.append({"symbol": k, "name": k, "price": v, "change": 0, "volume": 0})
    cache["all"] = result
    cache["hot"] = result[:100]
    cache["ready"] = True
    cache["last_update"] = time.time()
    print(f"[Cache] {len(result)} symbols")

# ═══════════════════════════════════════════════════════════════
#  BACKGROUND
# ═══════════════════════════════════════════════════════════════
async def background_fetcher():
    while True:
        try:
            await build_cache()
        except Exception as e:
            print(f"[BG ERROR] {e}")
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
    http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
    tasks = [
        asyncio.create_task(background_fetcher()),
        asyncio.create_task(ws_broadcaster())
    ]
    print("[Startup] Ready")
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
    return {"status": "running", "symbols": len(cache["all"]), "cmc_loaded": bool(CMC_API_KEY)}

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
    url = f"{BINANCE_KLINES_URL}?symbol={symbol.upper()}USDT&interval=1m&limit=50"
    data = await fetch_json(url)
    if not isinstance(data, list):
        return []
    return [
        {"time": c[0], "open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4])}
        for c in data if isinstance(c, list)
    ]

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    if len(ws_clients) >= MAX_WS_CLIENTS:
        await ws.close(code=1008)
        return
    await ws.accept()
    ws_clients.add(ws)
    if cache["ready"]:
        await ws.send_json({"type": "hot", "data": cache["hot"], "timestamp": int(time.time() * 1000)})
    try:
        while True:
            msg = await asyncio.wait_for(ws.receive_text(), timeout=30)
            if msg == "ping":
                await ws.send_json({"type": "pong"})
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    finally:
        ws_clients.discard(ws)
    
