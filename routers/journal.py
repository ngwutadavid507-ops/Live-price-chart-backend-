"""
/journal — trade journal CRUD.
Powers: journal.js.
"""

import json
import os
import time
import aiofiles
from fastapi import APIRouter, HTTPException
from models.journal import JournalCreateRequest
from utils.formatters import utc_now_iso

router = APIRouter()
JOURNAL_FILE = "data/journal.json"


async def _load() -> list[dict]:
    if os.path.exists(JOURNAL_FILE):
        async with aiofiles.open(JOURNAL_FILE, "r") as f:
            return json.loads(await f.read())
    return []


async def _save(data: list):
    os.makedirs("data", exist_ok=True)
    async with aiofiles.open(JOURNAL_FILE, "w") as f:
        await f.write(json.dumps(data, indent=2))


@router.get("/trades")
async def get_trades():
    entries = await _load()
    entries.sort(key=lambda x: x.get("date", ""), reverse=True)
    return {"count": len(entries), "trades": entries}


@router.post("/trades")
async def add_trade(req: JournalCreateRequest):
    entries = await _load()
    entry   = {
        "id":     int(time.time() * 1000),
        "date":   utc_now_iso()[:10],
        "pair":   req.pair.upper(),
        "side":   req.side,
        "entry":  req.entry,
        "exit":   req.exit or "—",
        "pnl":    req.pnl,
        "status": "success" if req.pnl > 0 else "loss",
        "tags":   req.tags,
        "note":   req.note,
    }
    entries.append(entry)
    await _save(entries)
    return {"status": "added", "trade": entry}


@router.put("/trades/{trade_id}")
async def update_trade(trade_id: int, updates: dict):
    entries = await _load()
    for e in entries:
        if e["id"] == trade_id:
            e.update(updates)
            if "pnl" in updates:
                e["status"] = "success" if updates["pnl"] > 0 else "loss"
            await _save(entries)
            return {"status": "updated", "trade": e}
    raise HTTPException(status_code=404, detail=f"Trade {trade_id} not found")


@router.delete("/trades/{trade_id}")
async def delete_trade(trade_id: int):
    entries = await _load()
    before  = len(entries)
    entries = [e for e in entries if e["id"] != trade_id]
    await _save(entries)
    if len(entries) == before:
        raise HTTPException(status_code=404, detail=f"Trade {trade_id} not found")
    return {"status": "deleted", "id": trade_id}


@router.get("/stats")
async def journal_stats():
    entries = await _load()
    if not entries:
        return {"total": 0, "win_rate": 0, "avg_pnl": 0, "total_pnl": 0}

    wins      = [e for e in entries if e.get("pnl", 0) > 0]
    total_pnl = sum(e.get("pnl", 0) for e in entries)

    return {
        "total":     len(entries),
        "wins":      len(wins),
        "losses":    len(entries) - len(wins),
        "win_rate":  round(len(wins) / len(entries) * 100, 2),
        "avg_pnl":   round(total_pnl / len(entries), 2),
        "total_pnl": round(total_pnl, 2),
              }
