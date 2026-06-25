"""
Binance Service — primary source for OHLCV and market scanning.
Uses public endpoints; no API key required for read operations.
"""

import httpx
from utils.safe_float import safe_float

BASE    = "https://api.binance.com"
TIMEOUT = 10


async def get_ticker_24h(symbol: str | None = None) -> list[dict]:
    url    = f"{BASE}/api/v3/ticker/24hr"
    params = {"symbol": symbol.upper()} if symbol else {}
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    if isinstance(data, dict):
        data = [data]
    return [
        {
            "symbol":    d["symbol"],
            "price":     safe_float(d["lastPrice"]),
            "change24h": safe_float(d["priceChangePercent"]),
            "volume":    safe_float(d["quoteVolume"]),
            "high":      safe_float(d["highPrice"]),
            "low":       safe_float(d["lowPrice"]),
            "source":    "binance",
        }
        for d in data
        if d["symbol"].endswith("USDT")
    ]


async def get_klines(symbol: str, interval: str = "1h", limit: int = 100) -> list[dict]:
    url    = f"{BASE}/api/v3/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        raw = r.json()
    return [
        {
            "time":   int(k[0]),
            "open":   safe_float(k[1]),
            "high":   safe_float(k[2]),
            "low":    safe_float(k[3]),
            "close":  safe_float(k[4]),
            "volume": safe_float(k[5]),
        }
        for k in raw
    ]


async def get_order_book(symbol: str, depth: int = 20) -> dict:
    url    = f"{BASE}/api/v3/depth"
    params = {"symbol": symbol.upper(), "limit": depth}
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        raw = r.json()
    return {
        "bids": [[safe_float(p), safe_float(q)] for p, q in raw["bids"]],
        "asks": [[safe_float(p), safe_float(q)] for p, q in raw["asks"]],
    }


async def get_recent_trades(symbol: str, limit: int = 50) -> list[dict]:
    url    = f"{BASE}/api/v3/trades"
    params = {"symbol": symbol.upper(), "limit": limit}
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        raw = r.json()
    return [
        {
            "price":    safe_float(t["price"]),
            "qty":      safe_float(t["qty"]),
            "is_buyer": t["isBuyerMaker"],
            "time":     int(t["time"]),
        }
        for t in raw
  ]
