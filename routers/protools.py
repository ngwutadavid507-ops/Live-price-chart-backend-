"""
/protools — order simulation, position sizing, risk metrics.
Powers: protools.js.
"""

from fastapi import APIRouter
from cache.market_cache import market_cache
from cache.analysis_cache import analysis_cache
from utils.validators import validate_symbol

router = APIRouter()


@router.post("/simulate")
async def simulate_order(order: dict):
    sym       = order.get("symbol", "").upper()
    side      = order.get("side", "long")
    size_usd  = float(order.get("size_usd", 100))
    leverage  = float(order.get("leverage", 1))
    sl_pct    = float(order.get("sl_pct", 2))
    tp_pct    = float(order.get("tp_pct", 4))

    asset = market_cache.get(sym)
    if not asset:
        return {"error": f"Symbol {sym} not in cache"}

    price    = asset["price"]
    position = size_usd * leverage
    qty      = position / price

    if side == "long":
        sl_price = round(price * (1 - sl_pct / 100), 6)
        tp_price = round(price * (1 + tp_pct / 100), 6)
    else:
        sl_price = round(price * (1 + sl_pct / 100), 6)
        tp_price = round(price * (1 - tp_pct / 100), 6)

    max_loss   = round(size_usd * sl_pct / 100, 2)
    max_profit = round(size_usd * tp_pct / 100 * leverage, 2)
    risk_reward = round(max_profit / max_loss, 2) if max_loss else 0

    return {
        "symbol":       sym,
        "side":         side,
        "entry_price":  price,
        "qty":          round(qty, 6),
        "position_usd": round(position, 2),
        "sl_price":     sl_price,
        "tp_price":     tp_price,
        "max_loss":     max_loss,
        "max_profit":   max_profit,
        "risk_reward":  risk_reward,
        "leverage":     leverage,
    }


@router.get("/risk/{symbol}")
async def risk_metrics(symbol: str):
    sym    = validate_symbol(symbol)
    result = await analysis_cache.get_or_compute(sym)
    asset  = market_cache.get(sym)

    if not result or not asset:
        return {"error": f"No data for {sym}"}

    price   = asset["price"]
    atr     = result.volatility.atr
    atr_pct = result.volatility.atr_pct

    return {
        "symbol":           sym,
        "price":            price,
        "atr":              round(atr, 6),
        "atr_pct":          round(atr_pct, 4),
        "suggested_sl":     round(price - atr * 1.5, 6),
        "suggested_tp":     round(price + atr * 3.0, 6),
        "bb_upper":         result.volatility.bb_upper,
        "bb_lower":         result.volatility.bb_lower,
        "bb_squeeze":       result.volatility.bb_squeeze,
        "volatility_label": (
            "high"   if atr_pct > 5 else
            "medium" if atr_pct > 2 else
            "low"
        ),
    }


@router.post("/position-size")
async def position_size(req: dict):
    account   = float(req.get("account_size", 1000))
    risk_pct  = float(req.get("risk_pct", 1))
    entry     = float(req.get("entry", 1))
    stop_loss = float(req.get("stop_loss", 0))

    if entry <= 0 or stop_loss <= 0 or entry == stop_loss:
        return {"error": "Invalid entry or stop_loss"}

    risk_usd = account * risk_pct / 100
    sl_dist  = abs(entry - stop_loss)
    sl_pct   = sl_dist / entry * 100
    qty      = risk_usd / sl_dist
    position = qty * entry

    return {
        "account_size": account,
        "risk_pct":     risk_pct,
        "risk_usd":     round(risk_usd, 2),
        "entry":        entry,
        "stop_loss":    stop_loss,
        "sl_distance":  round(sl_dist, 6),
        "sl_pct":       round(sl_pct, 4),
        "qty":          round(qty, 6),
        "position_usd": round(position, 2),
               }
