from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import aiohttp
import asyncio
import time
import os
from collections import defaultdict

# =========================
# CONFIG
# =========================
CMC_API_KEY = os.getenv("CMC_API_KEY")
CMC_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

CACHE_TTL_SECONDS = 10
WS_BROADCAST_INTERVAL = 5
RATE_LIMIT_WINDOW = 60
RATE_LIMIT_MAX_REQUESTS = 20
FETCH_TIMEOUT = 15

CMC_LIMIT_PER_PAGE = 500
CMC_MAX_PAGES = 5  # 500 * 5 = 2500 max safe fallback


# =========================
# STATE
# =========================
class AppState:
    def __init__(self):
        self.hot_cache = []
        self.all_cache = []
        self.last_update = 0
        self.ws_clients = set()
        self.rate_limits = defaultdict(list)
        self._lock = asyncio.Lock()

    async def update_cache(self, hot_data, all_data):
        async with self._lock:
            self.hot_cache = hot_data
            self.all_cache = all_data
            self.last_update = time.time()

    def get_hot(self):
        return self.hot_cache

    def get_all(self):
        return self.all_cache

    def check_rate_limit(self, ip):
        now = time.time()
        self.rate_limits[ip] = [
            t for t in self.rate_limits[ip]
            if now - t < RATE_LIMIT_WINDOW
        ]
        if len(self.rate_limits[ip]) >= RATE_LIMIT_MAX_REQUESTS:
            return False
        self.rate_limits[ip].append(now)
        return True


app_state = AppState()


# =========================
# SAFE CMC FETCH
# =========================
async def fetch_cmc_page(start=1, limit=500):
    if not CMC_API_KEY:
        return []

    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": start, "limit": limit, "convert": "USD"}

    try:
        timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(CMC_URL, headers=headers, params=params) as r:
                data = await r.json()

        return data.get("data", [])

    except Exception as e:
        print("CMC ERROR:", e)
        return []


async def fetch_all_cmc_safe():
    tasks = []
    for i in range(CMC_MAX_PAGES):
        start = i * CMC_LIMIT_PER_PAGE + 1
        tasks.append(fetch_cmc_page(start=start, limit=CMC_LIMIT_PER_PAGE))

    results = await asyncio.gather(*tasks)
    flat = []
    for r in results:
        flat.extend(r)

    return flat


# =========================
# TRANSFORMERS
# =========================
def transform(item):
    q = item.get("quote", {}).get("USD", {})
    return {
        "symbol": item.get("symbol"),
        "name": item.get("name"),
        "price": q.get("price", 0),
        "change": q.get("percent_change_24h", 0),
        "volume": q.get("volume_24h", 0),
    }


def transform_hot(item):
    q = item.get("quote", {}).get("USD", {})
    return {
        "symbol": item.get("symbol"),
        "price": q.get("price", 0),
        "change": q.get("percent_change_24h", 0),
    }


# =========================
# BACKGROUND FETCHER (FIXED)
# =========================
async def background_fetcher():
    while True:
        try:
            raw = await fetch_all_cmc_safe()

            hot = raw[:100]
            all_data = raw

            hot_data = [transform_hot(x) for x in hot]
            all_data = [transform(x) for x in all_data]

            await app_state.update_cache(hot_data, all_data)

        except Exception as e:
            print("Background error:", e)

        await asyncio.sleep(CACHE_TTL_SECONDS)


# =========================
# WEBSOCKET BROADCASTER (SAFE)
# =========================
async def ws_broadcaster():
    while True:
        await asyncio.sleep(WS_BROADCAST_INTERVAL)

        if not app_state.ws_clients:
            continue

        payload = {
            "type": "hot",
            "data": app_state.get_hot(),
            "timestamp": int(time.time() * 1000),
        }

        dead = set()

        for ws in list(app_state.ws_clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.add(ws)

        for ws in dead:
            app_state.ws_clients.discard(ws)


# =========================
# LIFESPAN
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(background_fetcher())
    asyncio.create_task(ws_broadcaster())
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================
# ROUTES
# =========================
@app.get("/")
async def home():
    return {"status": "running", "service": "Phoenix Terminal"}


@app.get("/health")
async def health():
    return {
        "ws_clients": len(app_state.ws_clients),
        "hot_cache": len(app_state.get_hot()),
        "all_cache": len(app_state.get_all()),
        "last_update": app_state.last_update,
    }


@app.get("/symbols")
async def symbols(request: Request):
    ip = request.client.host

    if not app_state.check_rate_limit(ip):
        raise HTTPException(429, "Rate limit exceeded")

    if not app_state.get_all():
        raw = await fetch_all_cmc_safe()
        await app_state.update_cache(
            [transform_hot(x) for x in raw[:100]],
            [transform(x) for x in raw]
        )

    return app_state.get_all()


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    app_state.ws_clients.add(ws)

    await ws.send_json({
        "type": "hot",
        "data": app_state.get_hot(),
        "timestamp": int(time.time() * 1000),
    })

    try:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=30)

                if msg == "ping":
                    await ws.send_json({"type": "pong"})
                else:
                    await ws.send_json({"type": "ack"})

            except asyncio.TimeoutError:
                await ws.send_json({"type": "keepalive"})

    except WebSocketDisconnect:
        pass
    finally:
        app_state.ws_clients.discard(ws)
