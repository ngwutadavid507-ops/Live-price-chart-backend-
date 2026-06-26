"""
WebSocket Manager — handles all client connections.
Broadcasts live price updates and signal fires.
"""

import asyncio
import json
from fastapi import WebSocket
from utils.formatters import timestamp_ms


class ConnectionManager:
    def __init__(self):
        self._clients: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._clients.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self._clients:
            self._clients.remove(ws)

    async def broadcast(self, payload: dict):
        dead    = []
        message = json.dumps(payload)
        for client in self._clients:
            try:
                await client.send_text(message)
            except Exception:
                dead.append(client)
        for d in dead:
            self.disconnect(d)

    async def broadcast_prices(self, hot_symbols: list[dict]):
        await self.broadcast({
            "type":      "price_update",
            "symbols":   hot_symbols,
            "timestamp": timestamp_ms(),
        })

    async def broadcast_signal(self, signal: dict):
        await self.broadcast({
            "type":      "signal_fire",
            "signal":    signal,
            "timestamp": timestamp_ms(),
        })

    async def broadcast_heartbeat(self):
        await self.broadcast({
            "type":      "heartbeat",
            "timestamp": timestamp_ms(),
        })

    def client_count(self) -> int:
        return len(self._clients)

    async def shutdown(self):
        for client in self._clients:
            try:
                await client.close()
            except Exception:
                pass
        self._clients.clear()


ws_manager = ConnectionManager()
