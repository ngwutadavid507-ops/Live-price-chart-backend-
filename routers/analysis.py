"""
/analysis — full technical analysis per symbol.
Powers: analysis.js — all indicator panels.
"""

from fastapi import APIRouter, Query
from cache.analysis_cache import analysis_cache
from utils.validators import validate_symbol

router = APIRouter()


@router.get("/{symbol}")
async def analyse(symbol: str):
    sym    = validate_symbol(symbol)
    result = await analysis_cache.get_or_compute(sym)
    if not result:
        return {"error": f"Unable to analyse {sym} — insufficient data"}
    return result


@router.get("/{symbol}/indicators")
async def indicators_only(symbol: str):
    sym    = validate_symbol(symbol)
    result = await analysis_cache.get_or_compute(sym)
    if not result:
        return {"error": "Insufficient data"}
    return {
        "symbol":     sym,
        "trend":      result.trend,
        "momentum":   result.momentum,
        "volatility": result.volatility,
        "volume":     result.volume,
    }


@router.get("/{symbol}/score")
async def market_score(symbol: str):
    sym    = validate_symbol(symbol)
    result = await analysis_cache.get_or_compute(sym)
    if not result:
        return {"symbol": sym, "market_score": 50.0}
    return {"symbol": sym, "market_score": result.market_score}
