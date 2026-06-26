"""
/compare — side-by-side multi-asset comparison.
Powers: compare.js.
"""

import asyncio
from fastapi import APIRouter, Query
from cache.market_cache import market_cache
from cache.analysis_cache import analysis_cache
from services.binance import get_klines
from utils.formatters import fmt_pct

router = APIRouter()


@router.get("")
async def compare_assets(
    symbols:  str = Query("BTCUSDT,ETHUSDT,SOLUSDT"),
    interval: str = Query("1d"),
    limit:    int = Query(30),
):
    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()][:6]

    candle_tasks   = [get_klines(s, interval=interval, limit=limit) for s in sym_list]
    analysis_tasks = [analysis_cache.get_or_compute(s) for s in sym_list]

    candle_results, analysis_results = await asyncio.gather(
        asyncio.gather(*candle_tasks,   return_exceptions=True),
        asyncio.gather(*analysis_tasks, return_exceptions=True),
    )

    comparisons = []
    for i, sym in enumerate(sym_list):
        asset    = market_cache.get(sym)
        analysis = analysis_results[i] if not isinstance(analysis_results[i], Exception) else None
        candles  = candle_results[i]   if not isinstance(candle_results[i],  Exception) else []

        closes     = [c["close"] for c in candles] if candles else []
        base       = closes[0] if closes else 1
        normalised = [round(c / base * 100, 4) for c in closes]

        entry = {
            "symbol":       sym,
            "price":        asset["price"]     if asset else 0,
            "change24h":    asset["change24h"] if asset else 0,
            "change24h_fmt": fmt_pct(asset["change24h"]) if asset else "N/A",
            "volume":       asset["volume"]    if asset else "—",
            "market_cap":   asset.get("market_cap", 0) if asset else 0,
            "source":       asset["source"]    if asset else "unknown",
            "performance":  normalised,
        }

        if analysis:
            entry.update({
                "trend":        analysis.trend.strength,
                "rsi":          analysis.momentum.rsi,
                "signal":       analysis.signal.direction,
                "confidence":   analysis.signal.confidence,
                "market_score": analysis.market_score,
                "atr_pct":      analysis.volatility.atr_pct,
                "volume_trend": analysis.volume.volume_trend,
            })

        comparisons.append(entry)

    ranked  = sorted(
        [c for c in comparisons if "market_score" in c],
        key=lambda x: x["market_score"],
        reverse=True,
    )
    rank_map = {r["symbol"]: i + 1 for i, r in enumerate(ranked)}
    for c in comparisons:
        c["rank"] = rank_map.get(c["symbol"], len(comparisons))

    return {
        "symbols":     sym_list,
        "comparisons": comparisons,
        "interval":    interval,
        "periods":     limit,
                         }
