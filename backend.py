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

@app.get("/symbols")
def symbols():
    return [
        {"symbol": "BTCUSDT", "price": 65000},
        {"symbol": "ETHUSDT", "price": 3500},
    ]

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    while True:
        await ws.send_json({
            "type": "prices",
            "data": [
                {
                    "symbol": "BTCUSDT",
                    "price": 65000 + int(time.time()) % 10
                },
                {
                    "symbol": "ETHUSDT",
                    "price": 3500 + int(time.time()) % 5
                }
            ]
        })

        await asyncio.sleep(1)
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    while True:
        await ws.send_json({
            "status": "connected"
        })

        await asyncio.sleep(1)

  @app.get("/health")
def health():
    return {
        "ws_route": "/ws",
        "status": "ok"
    }      
