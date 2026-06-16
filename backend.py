from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"status": "running"}

@app.get("/health")
def health():
    return {
        "status": "ok",
        "ws_route": "/ws"
    }

@app.get("/symbols")
def symbols():
    return [
        {"symbol": "BTCUSDT", "price": 65000},
        {"symbol": "ETHUSDT", "price": 3500}
    ]

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    try:
        while True:
            await ws.send_json({
                "type": "ping",
                "message": "connected",
                "time": time.time()
            })

            await asyncio.sleep(1)

    except Exception as e:
        print("WebSocket error:", e)
