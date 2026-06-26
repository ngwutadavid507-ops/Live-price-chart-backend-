"""
Ticker WebSocket — pushes live price updates to frontend every 2 seconds.
Frontend services/websocket.js connects to WS /markets/ws/ticker.
"""

import asyncio
from fastapi import WebSocket, WebSocketDisconnect
from websocket.manager import ws_manager
from cache.market_cache import market_cache


async def ticker_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    heartbeat_counter = 0
    try:
        while True:
            hot  = market_cache.hot()
            slim = [
                {
                    "symbol":    h["symbol"],
                    "price":     h["price"],
                    "change24h": h["change24h"],
                }
                for h in hot
            ]
            await ws_manager.broadcast_prices(slim)

            heartbeat_counter += 1
            if heartbeat_counter >= 5:
                await ws_manager.broadcast_heartbeat()
                heartbeat_counter = 0

            await asyncio.sleep(2)
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
    except Exception:
        ws_manager.disconnect(ws)
