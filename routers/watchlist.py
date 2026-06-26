"""
/watchlist — CRUD for user's saved symbols.
Powers: portfolio.js watchlist panel + watchlist.js.
"""

import json
import os
import aiofiles
from fastapi import APIRouter

router = APIRouter()
WATCHLIST_FILE = "data/watchlist.json"


async def _load() -> list[str]:
    if os.path.exists(WATCHLIST_FILE):
        async with aiofiles.open(WATCHLIST_FILE, "r") as f:
            return json.loads(await f.read())
    return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


async def _save(data: list):
    os.makedirs("data", exist_ok=True)
    async with aiofiles.open(WATCHLIST_FILE, "w") as f:
        await f.write(json.dumps(data))


@router.get("")
async def get_watchlist():
    return {"symbols": await _load()}


@router.post("/{symbol}")
async def add_to_watchlist(symbol: str):
    sym  = symbol.upper()
    data = await _load()
    if sym not in data:
        data.append(sym)
        await _save(data)
    return {"status": "added", "symbol": sym, "watchlist": data}


@router.delete("/{symbol}")
async def remove_from_watchlist(symbol: str):
    sym  = symbol.upper()
    data = await _load()
    data = [s for s in data if s != sym]
    await _save(data)
    return {"status": "removed", "symbol": sym, "watchlist": data}
