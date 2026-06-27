"""
/markets — debug version to identify candle source issue.
"""

from fastapi import APIRouter, Query, WebSocket
from cache.market_cache import market_cache
from cache.analysis_cache import analysis_cache
from services.bybit import get_klines as bybit_klines
from services.bybit import get_tickers
from utils.validators import validate_symbol, validate_timeframe, validate_limit
from models.market import MarketSummary
from websocket.ticker_ws import ticker_endpoint
import httpx

router = APIRouter()

INTERVAL_MAP = {
    "1m": "1", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "4h": "240", "1d": "D", "1w": "W",
}


@router.get("/summary")
async def market_summary():
    analyses = analysis_cache.all()
    if not analyses:
        assets = market_cache.all()
        return {
            "bullish_count":     0,
            "bearish_count":     0,
            "neutral_count":     len(assets),
            "avg_market_score":  50.0,
            "strongest_bullish": [],
            "strongest_bearish": [],
            "total_tracked":     len(assets),
        }
    bullish   = [a for a in analyses if a.signal.direction == "buy"]
    bearish   = [a for a in analyses if a.signal.direction == "sell"]
    neutral   = [a for a in analyses if a.signal.direction == "neutral"]
    avg_score = sum(a.market_score for a in analyses) / len(analyses) if analyses else 50.0
    return MarketSummary(
        bullish_count=len(bullish),
        bearish_count=len(bearish),
        neutral_count=len(neutral),
        avg_market_score=round(avg_score, 2),
        strongest_bullish=[a.symbol for a in sorted(bullish, key=lambda x: x.market_score, reverse=True)[:5]],
        strongest_bearish=[a.symbol for a in sorted(bearish, key=lambda x: x.market_score)[:5]],
        total_tracked=len(analyses),
    )


@router.get("/pairs")
async def all_pairs(
    limit:  int = Query(100, ge=1, le=2000),
    search: str = Query(""),
):
    assets = market_cache.all()
    if search:
        assets = [a for a in assets if search.upper() in a["symbol"]]
    assets = sorted(assets, key=lambda x: x["volume_raw"], reverse=True)
    return {"count": len(assets), "assets": assets[:limit]}


@router.get("/ticker")
async def ticker():
    return {"symbols": market_cache.hot()}


@router.get("/trending")
async def trending(limit: int = Query(20, ge=1, le=50)):
    analyses = analysis_cache.all()
    if analyses:
        ranked = sorted(analyses, key=lambda x: x.market_score, reverse=True)[:limit]
        return {"ranked_by": "market_score", "assets": [
            {"symbol": a.symbol, "market_score": a.market_score,
             "direction": a.signal.direction}
            for a in ranked
        ]}
    hot = market_cache.hot()[:limit]
    return {"ranked_by": "volume", "assets": hot}


@router.get("/candles/{symbol}")
async def candles(
    symbol:   str,
    interval: str = Query("1h"),
    limit:    int  = Query(100, ge=10, le=500),
):
    sym            = validate_symbol(symbol)
    tf             = validate_timeframe(interval)
    lim            = validate_limit(limit)
    bybit_interval = INTERVAL_MAP.get(tf, "60")

    # Debug: test Bybit directly
    debug_info = {}
    try:
        url    = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "linear",
            "symbol":   sym,
            "interval": bybit_interval,
            "limit":    5,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, params=params)
            debug_info["status_code"] = r.status_code
            debug_info["raw"]         = r.json()
    except Exception as e:
        debug_info["error"] = str(e)

    bars = await bybit_klines(sym, interval=bybit_interval, limit=lim)

    return {
        "symbol":     sym,
        "interval":   tf,
        "count":      len(bars),
        "candles":    bars,
        "debug":      debug_info,
    }


@router.websocket("/ws/ticker")
async def ws_ticker(ws: WebSocket):
    await ticker_endpoint(ws)
