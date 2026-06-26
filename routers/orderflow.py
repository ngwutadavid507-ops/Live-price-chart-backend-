"""
/orderflow — L2 order book, bid/ask imbalance, CVD.
Powers: orderflow.js — depth matrix, pressure bars.
"""

from fastapi import APIRouter, Query
from services.binance import get_order_book as binance_ob, get_recent_trades
from services.bybit import get_order_book as bybit_ob
from engines.whale_engine import order_book_imbalance
from utils.validators import validate_symbol

router = APIRouter()


@router.get("/{symbol}")
async def orderflow(
    symbol: str,
    source: str = Query("binance", description="binance | bybit"),
    depth:  int = Query(20, ge=5, le=100),
):
    sym = validate_symbol(symbol)

    if source == "bybit":
        book = await bybit_ob(sym, depth=depth)
    else:
        book = await binance_ob(sym, depth=depth)

    imbalance = order_book_imbalance(book["bids"], book["asks"])
    trades    = await get_recent_trades(sym, limit=50)

    buy_vol  = sum(t["qty"] for t in trades if not t["is_buyer"])
    sell_vol = sum(t["qty"] for t in trades if t["is_buyer"])
    cvd      = round(buy_vol - sell_vol, 4)

    return {
        "symbol":    sym,
        "bids":      book["bids"],
        "asks":      book["asks"],
        "imbalance": imbalance,
        "cvd":       cvd,
        "buy_vol":   round(buy_vol, 4),
        "sell_vol":  round(sell_vol, 4),
    }


@router.get("/{symbol}/book")
async def order_book_only(symbol: str, depth: int = Query(20)):
    sym  = validate_symbol(symbol)
    book = await binance_ob(sym, depth=depth)
    return {"symbol": sym, **book}
