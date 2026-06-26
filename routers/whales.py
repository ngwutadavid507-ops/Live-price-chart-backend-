"""
/whales — large trade detection and alerts.
Powers: orderflow.js whale panel + whales.js (Phase 3).
"""

from fastapi import APIRouter, Query
from services.binance import get_recent_trades
from engines.whale_engine import detect_whales
from cache.market_cache import market_cache
from utils.validators import validate_symbol

router = APIRouter()

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]


@router.get("/alerts")
async def whale_alerts(
    symbols:   str   = Query(",".join(DEFAULT_SYMBOLS)),
    threshold: float = Query(100_000),
):
    sym_list   = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    all_alerts = []

    for sym in sym_list[:10]:
        asset = market_cache.get(sym)
        price = asset["price"] if asset else 0.0
        try:
            trades = await get_recent_trades(sym, limit=50)
            alerts = detect_whales(trades, price)
            for alert in alerts:
                alert["symbol"] = sym
                if alert["value_usd"] >= threshold:
                    all_alerts.append(alert)
        except Exception:
            continue

    all_alerts.sort(key=lambda x: x["value_usd"], reverse=True)
    return {"count": len(all_alerts), "alerts": all_alerts[:50]}


@router.get("/{symbol}")
async def symbol_whales(symbol: str, limit: int = Query(20)):
    sym    = validate_symbol(symbol)
    asset  = market_cache.get(sym)
    price  = asset["price"] if asset else 0.0
    trades = await get_recent_trades(sym, limit=100)
    alerts = detect_whales(trades, price)
    return {"symbol": sym, "count": len(alerts), "alerts": alerts[:limit]}
