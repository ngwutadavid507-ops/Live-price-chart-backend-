"""
/correlation — price correlation matrix between assets.
Powers: correlation.js — correlation matrix heatmap.
"""

import asyncio
from fastapi import APIRouter, Query
from services.binance import get_klines
from utils.validators import validate_symbol

router = APIRouter()

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT"]


def _pearson(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 2:
        return 0.0
    a, b   = a[-n:], b[-n:]
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    num    = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
    den_a  = sum((x - mean_a) ** 2 for x in a) ** 0.5
    den_b  = sum((y - mean_b) ** 2 for y in b) ** 0.5
    if den_a == 0 or den_b == 0:
        return 0.0
    return round(num / (den_a * den_b), 4)


def _beta(asset_returns: list[float], market_returns: list[float]) -> float:
    n = min(len(asset_returns), len(market_returns))
    if n < 2:
        return 1.0
    a      = asset_returns[-n:]
    m      = market_returns[-n:]
    mean_m = sum(m) / n
    cov    = sum((ai - sum(a)/n) * (mi - mean_m) for ai, mi in zip(a, m)) / n
    var_m  = sum((mi - mean_m) ** 2 for mi in m) / n
    return round(cov / var_m, 4) if var_m else 1.0


def _pct_returns(closes: list[float]) -> list[float]:
    return [
        (closes[i] - closes[i - 1]) / closes[i - 1]
        for i in range(1, len(closes))
        if closes[i - 1] != 0
    ]


@router.get("/matrix")
async def correlation_matrix(
    symbols:  str = Query(",".join(DEFAULT_SYMBOLS)),
    interval: str = Query("1d"),
    limit:    int = Query(90),
):
    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()][:12]
    tasks    = [get_klines(s, interval=interval, limit=limit) for s in sym_list]
    results  = await asyncio.gather(*tasks, return_exceptions=True)

    closes_map: dict[str, list[float]] = {}
    for sym, result in zip(sym_list, results):
        if isinstance(result, list) and result:
            closes_map[sym] = [c["close"] for c in result]

    valid_syms = list(closes_map.keys())

    matrix = []
    for sym_a in valid_syms:
        row = []
        for sym_b in valid_syms:
            if sym_a == sym_b:
                row.append(1.0)
            else:
                row.append(_pearson(closes_map[sym_a], closes_map[sym_b]))
        matrix.append(row)

    btc_returns = _pct_returns(closes_map.get("BTCUSDT", [1, 1]))
    betas = {
        sym: _beta(_pct_returns(closes_map[sym]), btc_returns)
        for sym in valid_syms
    }

    return {
        "symbols":  valid_syms,
        "matrix":   matrix,
        "betas":    betas,
        "interval": interval,
        "periods":  limit,
    }


@router.get("/pair")
async def pair_correlation(
    symbol_a: str = Query("ETHUSDT"),
    symbol_b: str = Query("BTCUSDT"),
    interval: str = Query("1d"),
    limit:    int = Query(90),
):
    sym_a = validate_symbol(symbol_a)
    sym_b = validate_symbol(symbol_b)

    candles_a, candles_b = await asyncio.gather(
        get_klines(sym_a, interval=interval, limit=limit),
        get_klines(sym_b, interval=interval, limit=limit),
    )

    closes_a = [c["close"] for c in candles_a]
    closes_b = [c["close"] for c in candles_b]
    corr     = _pearson(closes_a, closes_b)
    beta     = _beta(_pct_returns(closes_a), _pct_returns(closes_b))

    label = (
        "strong positive"   if corr >= 0.7  else
        "moderate positive" if corr >= 0.4  else
        "weak"              if corr >= -0.4 else
        "moderate negative" if corr >= -0.7 else
        "strong negative"
    )

    return {
        "symbol_a":    sym_a,
        "symbol_b":    sym_b,
        "correlation": corr,
        "beta":        beta,
        "label":       label,
        "interval":    interval,
        "periods":     limit,
    }
