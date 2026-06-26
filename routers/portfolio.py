"""
/portfolio — paper portfolio positions, P&L, risk metrics.
Powers: portfolio.js.
"""

import json
import os
import aiofiles
from fastapi import APIRouter
from cache.market_cache import market_cache

router = APIRouter()
PORTFOLIO_FILE = "data/portfolio.json"

DEFAULT_PORTFOLIO = {
    "balance":     10000.0,
    "pnl_24h":    0.0,
    "pct_24h":    0.0,
    "positions":  [],
    "total_value": 10000.0,
}


async def _load() -> dict:
    if os.path.exists(PORTFOLIO_FILE):
        async with aiofiles.open(PORTFOLIO_FILE, "r") as f:
            return json.loads(await f.read())
    return DEFAULT_PORTFOLIO.copy()


async def _save(data: dict):
    os.makedirs("data", exist_ok=True)
    async with aiofiles.open(PORTFOLIO_FILE, "w") as f:
        await f.write(json.dumps(data, indent=2))


@router.get("")
async def get_portfolio():
    data            = await _load()
    total_pos_value = 0.0
    for pos in data.get("positions", []):
        asset = market_cache.get(pos["symbol"] + "USDT")
        if asset:
            pos["current_price"] = asset["price"]
            pos["pnl"]           = round((asset["price"] - pos["entry_price"]) * pos["amount"], 2)
            pos["pnl_pct"]       = round((asset["price"] / pos["entry_price"] - 1) * 100, 2)
        total_pos_value += pos.get("amount", 0) * pos.get("current_price", 0)
    data["total_value"] = round(data["balance"] + total_pos_value, 2)
    return data


@router.post("/position")
async def add_position(position: dict):
    data = await _load()
    data.setdefault("positions", []).append(position)
    await _save(data)
    return {"status": "added", "position": position}


@router.delete("/position/{symbol}")
async def remove_position(symbol: str):
    data   = await _load()
    before = len(data.get("positions", []))
    data["positions"] = [p for p in data.get("positions", []) if p["symbol"] != symbol.upper()]
    await _save(data)
    return {"status": "removed", "removed": before - len(data["positions"])}
