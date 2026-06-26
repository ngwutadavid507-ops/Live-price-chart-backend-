"""
/signals — signal feed and per-symbol signals.
Powers: signals.js signal feed + dashboard recent signals panel.
"""

import asyncio
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
from cache.analysis_cache import analysis_cache
from utils.validators import validate_symbol
from websocket.manager import ws_manager

router = APIRouter()


@router.get("/scan")
async def scan_signals(min_confidence: float = Query(68.0, ge=0, le=100)):
    analyses = analysis_cache.all()
    fired = [
        {
            "symbol":       a.symbol,
            "direction":    a.signal.direction,
            "confidence":   a.signal.confidence,
            "agreements":   a.signal.agreements,
            "entry":        a.signal.entry,
            "stop_loss":    a.signal.stop_loss,
            "take_profit":  a.signal.take_profit,
            "market_score": a.market_score,
            "trend":        a.trend.strength,
            "timestamp":    a.timestamp,
        }
        for a in analyses
        if a.signal.fired and a.signal.confidence >= min_confidence
    ]
    fired.sort(key=lambda x: x["confidence"], reverse=True)
    return {"count": len(fired), "signals": fired}


@router.get("/{symbol}")
async def symbol_signal(symbol: str):
    sym    = validate_symbol(symbol)
    result = await analysis_cache.get_or_compute(sym)
    if not result:
        return {"error": f"No data for {sym}"}
    return {
        "symbol":    sym,
        "signal":    result.signal,
        "trend":     result.trend,
        "timestamp": result.timestamp,
    }


@router.websocket("/ws/signals")
async def ws_signals(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        while True:
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
