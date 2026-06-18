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
FETCH_TIMEOUT = 15

CMC_LIMIT = 2500  # 🔥 FIXED (your confirmed working size)


# =========================
# STATE
# =========================
class AppState:
    def __init__(self):
        self.hot_cache = []
        self.all_cache = []
        self.last_update = 0
        self.ws_clients = set()
        self._lock = asyncio.Lock()

    async def update_cache(self, hot, all_):
        async with self._lock:
            self.hot_cache = hot
            self.all_cache = all_
            self.last_update = time.time()

    def hot(self):
        return self.hot_cache

    def all(self):
        return self.all_cache


app_state = AppState()


# =========================
# FETCH CMC (SAFE)
# =========================
async def fetch_cmc(limit=100):
    if not CMC_API_KEY:
        return []

    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": 1, "limit": limit, "convert": "USD"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                CMC_URL,
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
            ) as r:

                if r.status != 200:
                    return []

                data = await r.json()
                return data.get("data", [])

    except Exception as e:
        print("CMC ERROR:", e)
        return []


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
# BACKGROUND UPDATER
# =========================
async def updater():
    while True:
        raw = await fetch_cmc(CMC_LIMIT)

        if raw:
            hot = [transform_hot(x) for x in raw[:50]]
            all_ = [transform(x) for x in raw]

            await app_state.update_cache(hot, all_)

        await asyncio.sleep(CACHE_TTL_SECONDS)


# =========================
# BROADCAST
# =========================
async def broadcaster():
    while True:
        await asyncio.sleep(WS_BROADCAST_INTERVAL)

        if not app_state.ws_clients:
            continue

        payload = {
            "type": "hot",
            "data": app_state.hot(),
            "time": time.time()
        }

        dead = set()

        for ws in app_state.ws_clients:
            try:
                await ws.send_json(payload)
            except:
                dead.add(ws)

        for d in dead:
            app_state.ws_clients.discard(d)


# =========================
# APP
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(updater())
    asyncio.create_task(broadcaster())
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
    return {"status": "running"}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "ws_clients": len(app_state.ws_clients),
        "hot_cache": len(app_state.hot()),
        "all_cache": len(app_state.all()),
        "last_update": app_state.last_update,
    }


@app.get("/symbols")
async def symbols():
    data = app_state.all()

    if not data:
        raw = await fetch_cmc(CMC_LIMIT)
        if not raw:
            raise HTTPException(500, "CMC fetch failed")

        data = [transform(x) for x in raw]
        hot = [transform_hot(x) for x in raw[:50]]

        await app_state.update_cache(hot, data)

    return data


# =========================
# WEBSOCKET
# =========================
@app.websocket("/ws")
async def ws(ws: WebSocket):
    await ws.accept()
    app_state.ws_clients.add(ws)

    try:
        await ws.send_json({"type": "hot", "data": app_state.hot()})

        while True:
            try:
                msg = await asyncio.wait_for(ws.receive_text(), timeout=30)
                if msg == "ping":
                    await ws.send_json({"type": "pong"})
            except asyncio.TimeoutError:
                await ws.send_json({"type": "keepalive"})

    except WebSocketDisconnect:
        pass
    finally:
        app_state.ws_clients.discard(ws)
