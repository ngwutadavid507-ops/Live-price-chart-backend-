"""
Bybit WebSocket Service — live price stream.
Connects to Bybit WSS (not blocked on Render).
Feeds prices directly into market_cache.
"""

import asyncio
import json
import websockets
from utils.safe_float import safe_float

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT",
    "UNIUSDT", "LTCUSDT", "ATOMUSDT", "NEARUSDT", "MATICUSDT",
    "APTUSDT", "ARBUSDT", "OPUSDT", "SUIUSDT", "PEPEUSDT",
]

WS_URL = "wss://stream.bybit.com/v5/public/linear"


async def start_bybit_ws(cache_callback):
    """
    Connect to Bybit WebSocket and call cache_callback
    with each price update.
    Reconnects automatically on disconnect.
    """
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=10,
