"""
/markets — CoinGecko candles + WebSocket live prices.
"""

from fastapi import APIRouter, Query, WebSocket
from cache.market_cache import market_cache
from cache.analysis_cache import analysis_cache, _get_candles
from utils.validators import validate_symbol, validate_timeframe, validate_limit
from models.market import MarketSummary
from websocket.ticker_ws import ticker_endpoint

router = APIRouter()


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
    avg_score = sum(a.market_score for a in analyses) / len(analyses)
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
    return {"ranked_by": "volume", "assets": market_cache.hot()[:limit]}


@router.get("/candles/{symbol}")
async def candles(
    symbol:   str,
    interval: str = Query("1h"),
    limit:    int  = Query(100, ge=10, le=500),
):
    sym  = validate_symbol(symbol)
    tf   = validate_timeframe(interval)
    lim  = validate_limit(limit)
    bars = await _get_candles(sym)
    return {
        "symbol":   sym,
        "interval": tf,
        "count":    len(bars),
        "candles":  bars[-lim:],
        "source":   "coingecko",
    }


@router.websocket("/ws/ticker")
async def ws_ticker(ws: WebSocket):
    await ticker_endpoint(ws)
