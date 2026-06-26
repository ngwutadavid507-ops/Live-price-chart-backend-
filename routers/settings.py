"""
/settings — user preferences persistence.
Powers: settings.js.
"""

import json
import os
import aiofiles
from fastapi import APIRouter

router = APIRouter()
SETTINGS_FILE = "data/settings.json"

DEFAULT_SETTINGS = {
    "theme":                   "dark",
    "default_symbol":          "BTCUSDT",
    "default_interval":        "1h",
    "default_watchlist":       ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "signal_alerts":           True,
    "whale_alerts":            True,
    "min_signal_confidence":   68.0,
    "whale_threshold_usd":     100000,
    "layout":                  "default",
    "currency":                "USD",
}


async def _load() -> dict:
    if os.path.exists(SETTINGS_FILE):
        async with aiofiles.open(SETTINGS_FILE, "r") as f:
            saved = json.loads(await f.read())
            return {**DEFAULT_SETTINGS, **saved}
    return DEFAULT_SETTINGS.copy()


async def _save(data: dict):
    os.makedirs("data", exist_ok=True)
    async with aiofiles.open(SETTINGS_FILE, "w") as f:
        await f.write(json.dumps(data, indent=2))


@router.get("")
async def get_settings():
    return await _load()


@router.put("")
async def update_settings(updates: dict):
    current = await _load()
    current.update(updates)
    await _save(current)
    return {"status": "saved", "settings": current}


@router.post("/reset")
async def reset_settings():
    await _save(DEFAULT_SETTINGS.copy())
    return {"status": "reset", "settings": DEFAULT_SETTINGS}
