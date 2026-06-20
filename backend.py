from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import aiohttp
import asyncio
import json
import time
import random
from collections import defaultdict
from typing import Optional, List, Dict, Any

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_CHART = "https://api.coingecko.com/api/v3/coins/{id}/market_chart"
COINPAPRIKA_TICKERS = "https://api.coinpaprika.com/v1/tickers"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers?category=spot"

CACHE_TTL = 30
WS_INTERVAL = 3
RATE_LIMIT_MAX = 30
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
async def fetch_json(url: str, headers: Optional[Dict] = None, params: Optional[Dict] = None, timeout: int = 15) -> Optional[Any]:
    global http_session
    if http_session is None:
        return None
    try:
        async with http_session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            text = await r.text()
            if r.status == 429:
                print(f"[FETCH] RATE LIMITED: {url[:80]}")
                return None
            if r.status == 404:
                return None
            if r.status != 200:
                print(f"[FETCH] HTTP {r.status}: {text[:120]}")
                return None
            return json.loads(text)
    except asyncio.TimeoutError:
        print(f"[FETCH] TIMEOUT: {url[:80]}")
        return None
    except Exception as e:
        print(f"[FETCH] ERROR: {type(e).__name__}: {str(e)[:80]}")
        return None

# ═══════════════════════════════════════════════════════════════
#  SOURCE 1: BYBIT (EXCHANGE - PRIMARY FOR TRADING)
# ═══════════════════════════════════════════════════════════════
async def fetch_bybit() -> tuple:
    """Bybit is PRIMARY - this is the price you actually trade"""
    data = await fetch_json(BYBIT_TICKERS, timeout=15)
    if not isinstance(data, dict):
        return [], {}
    result = data.get("result", {})
    tickers = result.get("list", [])
    if not isinstance(tickers, list):
        return [], {}

    bybit_list = []
    bybit_map = {}
    for t in tickers:
        if not isinstance(t, dict):
            continue
        # FIX: only accept symbols that end with USDT, strip cleanly
        raw_symbol = t.get("symbol", "")
        if not raw_symbol.endswith("USDT"):
            continue
        symbol = raw_symbol[:-4].upper()
        if not symbol or len(symbol) > 10:
            continue
        price = float(t.get("lastPrice") or 0)
        if price <= 0:
            continue
        change = float(t.get("price24hPcnt") or 0) * 100
        high = float(t.get("highPrice24h") or 0)
        low = float(t.get("lowPrice24h") or 0)
        volume = float(t.get("turnover24h") or 0)

        coin = {
            "symbol": symbol,
            "name": symbol,  # Will be filled by CoinGecko
            "price": price,
            "change": change,
            "volume": volume,
            "market_cap": 0,
            "high_24h": high if high > 0 else price * 1.02,
            "low_24h": low if low > 0 else price * 0.98,
            "source": "bybit",
            "price_confidence": "exchange"
        }
        bybit_list.append(coin)
        bybit_map[symbol] = coin
    print(f"[BYBIT] Fetched {len(bybit_list)} tickers")
    return bybit_list, bybit_map

# ═══════════════════════════════════════════════════════════════
#  SOURCE 2: COINGECKO (AGGREGATOR - METADATA + MISSING COINS)
# ═══════════════════════════════════════════════════════════════
async def fetch_coingecko() -> tuple:
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h"
    }
    data = await fetch_json(COINGECKO_MARKETS, params=params, timeout=20)
    if not isinstance(data, list):
        return [], {}
    result = []
    id_map = {}
    for coin in data:
        if not isinstance(coin, dict):
            continue
        symbol = coin.get("symbol", "").upper()
        cg_id = coin.get("id")
        result.append({
            "symbol": symbol,
            "name": coin.get("name"),
            "price": float(coin.get("current_price") or 0),
            "change": float(coin.get("price_change_percentage_24h") or 0),
            "volume": float(coin.get("total_volume") or 0),
            "market_cap": float(coin.get("market_cap") or 0),
            "high_24h": float(coin.get("high_24h") or 0),
            "low_24h": float(coin.get("low_24h") or 0),
            "source": "coingecko",
            "price_confidence": "aggregator"
        })
        # FIX: store the actual CoinGecko id correctly
        id_map[symbol] = cg_id
    print(f"[COINGECKO] Fetched {len(result)} coins")
    return result, id_map

# ═══════════════════════════════════════════════════════════════
#  SOURCE 3: COINPAPRIKA (BACKUP)
# ═══════════════════════════════════════════════════════════════
async def fetch_coinpaprika() -> List[Dict]:
    data = await fetch_json(COINPAPRIKA_TICKERS, timeout=20)
    if not isinstance(data, list):
        return []
    result = []
    for coin in data[:250]:
        if not isinstance(coin, dict):
            continue
        quotes = coin.get("quotes", {})
        usd = quotes.get("USD", {})
        if not usd:
            continue
        symbol = coin.get("symbol", "").upper()
        price = float(usd.get("price") or 0)
        change = float(usd.get("percent_change_24h") or 0)
        estimated_high = price * (1 + abs(change) / 100 * 0.6) if price > 0 else 0
        estimated_low = price * (1 - abs(change) / 100 * 0.6) if price > 0 else 0
        result.append({
            "symbol": symbol,
            "name": coin.get("name"),
            "price": price,
            "change": change,
            "volume": float(usd.get("volume_24h") or 0),
            "market_cap": float(usd.get("market_cap") or 0),
            "high_24h": estimated_high,
            "low_24h": estimated_low,
            "source": "coinpaprika",
            "price_confidence": "backup"
        })
    print(f"[COINPAPRIKA] Fetched {len(result)} coins")
    return result

# ═══════════════════════════════════════════════════════════════
#  MERGE SOURCES (TRADING-FIRST: Bybit > CoinGecko > CoinPaprika)
# ═══════════════════════════════════════════════════════════════
def merge_sources(bb_data: List[Dict], bb_map: Dict, cg_data: List[Dict], cg_id_map: Dict, cp_data: List[Dict]) -> tuple:
    """
    TRADING-FIRST MERGE:
    1. Bybit (exchange) = PRIMARY price - this is what you trade
    2. CoinGecko = name, market_cap, volume, id_map
    3. CoinPaprika = only if both above fail

    Never average exchange price with aggregator price!
    """
    merged = {}
    id_map = {}

    # 1. Start with Bybit (exchange prices) - THE TRUTH for trading
    for coin in bb_data:
        sym = coin["symbol"]
        merged[sym] = coin.copy()

    # 2. CoinGecko adds metadata + fills coins not on Bybit
    for coin in cg_data:
        sym = coin["symbol"]
        # FIX: use cg_id_map (passed in) instead of coin.get("cg_id") which doesn't exist
        if cg_id_map.get(sym):
            id_map[sym] = cg_id_map[sym]

        if sym not in merged:
            merged[sym] = coin.copy()
        else:
            # Coin exists on Bybit - ONLY add metadata, NEVER touch price
            if coin.get("name") and merged[sym].get("name") == sym:
                merged[sym]["name"] = coin["name"]
            if coin.get("market_cap", 0) > 0:
                merged[sym]["market_cap"] = coin["market_cap"]
            if coin.get("volume", 0) > 0 and merged[sym].get("volume", 0) == 0:
                merged[sym]["volume"] = coin["volume"]
            if merged[sym].get("high_24h", 0) == 0 and coin.get("high_24h", 0) > 0:
                merged[sym]["high_24h"] = coin["high_24h"]
            if merged[sym].get("low_24h", 0) == 0 and coin.get("low_24h", 0) > 0:
                merged[sym]["low_24h"] = coin["low_24h"]

    # 3. CoinPaprika - only for coins missing from both
    for coin in cp_data:
        sym = coin["symbol"]
        if sym not in merged:
            merged[sym] = coin.copy()

    # 4. Final safety: ensure no zero high/low
    for sym, coin in merged.items():
        price = coin.get("price", 0)
        change = coin.get("change", 0)
        if price > 0:
            if coin.get("high_24h", 0) == 0:
                coin["high_24h"] = price * (1 + abs(change) / 100 * 0.5)
            if coin.get("low_24h", 0) == 0:
                coin["low_24h"] = price * (1 - abs(change) / 100 * 0.5)
            if coin["high_24h"] <= coin["low_24h"]:
                coin["high_24h"] = price * 1.01
                coin["low_24h"] = price * 0.99

    result = list(merged.values())
    result.sort(key=lambda x: (x.get("price_confidence") == "exchange", x.get("volume", 0)), reverse=True)
    return result, id_map

# ═══════════════════════════════════════════════════════════════
#  BUILD CACHE
# ═══════════════════════════════════════════════════════════════
async def build_cache():
    global cache

    bb_task = asyncio.create_task(fetch_bybit())
    cg_task = asyncio.create_task(fetch_coingecko())
    cp_task = asyncio.create_task(fetch_coinpaprika())

    bb_result, cg_result, cp_result = await asyncio.gather(
        bb_task, cg_task, cp_task, return_exceptions=True
    )

    bb_data = [] if isinstance(bb_result, Exception) else bb_result[0] if isinstance(bb_result, tuple) else []
    bb_map = {} if isinstance(bb_result, Exception) else bb_result[1] if isinstance(bb_result, tuple) and len(bb_result) > 1 else {}
    cg_data, cg_id_map = ([], {}) if isinstance(cg_result, Exception) else cg_result
    cp_data = [] if isinstance(cp_result, Exception) else cp_result

    print(f"[BUILD] BYBIT:{len(bb_data)} CG:{len(cg_data)} CP:{len(cp_data)}")

    if not bb_data and not cg_data and not cp_data:
        print("[CACHE] ALL SOURCES FAILED")
        if not cache["all"]:
            cache["ready"] = True
        return

    merged, id_map = merge_sources(bb_data, bb_map, cg_data, cg_id_map, cp_data)

    cache["all"] = merged
    cache["hot"] = merged[:100]
    cache["id_map"] = id_map
    cache["ready"] = True
    cache["last_update"] = time.time()

    exchange_count = sum(1 for c in merged if c.get("price_confidence") == "exchange")
    print(f"[CACHE] Built: {len(merged)} symbols | {exchange_count} from exchange | {len(merged)-exchange_count} from aggregators")

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
    print(f"[STARTUP] Phoenix Trading Backend v5 - Exchange-First Priority")
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
    exchange_count = sum(1 for c in cache["all"] if c.get("price_confidence") == "exchange")
    return {
        "status": "running",
        "symbols": len(cache["all"]),
        "exchange_prices": exchange_count,
        "aggregator_prices": len(cache["all"]) - exchange_count,
        "sources": ["bybit", "coingecko", "coinpaprika"],
        "priority": "exchange-first",
        "cache_age": int(time.time() - cache.get("last_update", 0))
    }

@app.get("/health")
async def health():
    exchange_count = sum(1 for c in cache["all"] if c.get("price_confidence") == "exchange")
    return {
        "status": "ok",
        "cache_ready": cache["ready"],
        "symbols": len(cache["all"]),
        "exchange_prices": exchange_count,
        "ws_clients": len(ws_clients),
        "sources": ["bybit", "coingecko", "coinpaprika"],
        "priority": "exchange-first",
        "cache_age_seconds": int(time.time() - cache.get("last_update", 0))
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
        return generate_demo_candles(symbol_upper)

    url = COINGECKO_CHART.format(id=coin_id)
    params = {"vs_currency": "usd", "days": days}

    data = await fetch_json(url, params=params, timeout=20)

    if not data or not isinstance(data, dict):
        return generate_demo_candles(symbol_upper)

    prices = data.get("prices", [])
    if not prices or len(prices) < 10:
        return generate_demo_candles(symbol_upper)

    total_points = len(prices)
    candles_per_agg = max(1, total_points // 100)

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

    return candles

def generate_demo_candles(symbol="BTC"):
    seed = sum(ord(c) for c in symbol)
    rng = random.Random(seed)
    now = int(time.time() * 1000)
    base_price = 50000 + rng.random() * 50000
    if symbol == "BTC": base_price = 63900
    elif symbol == "ETH": base_price = 3450
    elif symbol == "SOL": base_price = 145

    candles = []
    for i in range(100):
        ts = now - (100 - i) * 3600 * 1000
        change = (rng.random() - 0.48) * 0.02
        open_p = base_price
        close_p = base_price * (1 + change)
        high_p = max(open_p, close_p) * (1 + rng.random() * 0.005)
        low_p = min(open_p, close_p) * (1 - rng.random() * 0.005)
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
