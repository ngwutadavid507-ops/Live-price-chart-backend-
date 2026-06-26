"""
Binance Service — with headers to bypass Render IP restrictions.
"""

import httpx
from utils.safe_float import safe_float

BASE    = "https://api.binance.com"
TIMEOUT = 15

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


async def get_ticker_24h(symbol: str | None = None) -> list[dict]:
    url    = f"{BASE}/api/v3/ticker/24hr"
    params = {"symbol": symbol.upper()} if symbol else {}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
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
    except Exception:
        return []


async def get_klines(symbol: str, interval: str = "1h", limit: int = 100) -> list[dict]:
    url    = f"{BASE}/api/v3/klines"
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
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
    except Exception:
        return await _bybit_klines_fallback(symbol, interval, limit)


async def _bybit_klines_fallback(symbol: str, interval: str, limit: int) -> list[dict]:
    """Bybit fallback when Binance candles fail."""
    interval_map = {
        "1m": "1", "5m": "5", "15m": "15", "30m": "30",
        "1h": "60", "4h": "240", "1d": "D", "1w": "W",
    }
    bybit_interval = interval_map.get(interval, "60")
    url    = "https://api.bybit.com/v5/market/kline"
    params = {
        "category": "linear",
        "symbol":   symbol.upper(),
        "interval": bybit_interval,
        "limit":    limit,
    }
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            raw = r.json()
        items = raw.get("result", {}).get("list", [])
        return [
            {
                "time":   int(k[0]),
                "open":   safe_float(k[1]),
                "high":   safe_float(k[2]),
                "low":    safe_float(k[3]),
                "close":  safe_float(k[4]),
                "volume": safe_float(k[5]),
            }
            for k in reversed(items)
        ]
    except Exception:
        return []


async def get_order_book(symbol: str, depth: int = 20) -> dict:
    url    = f"{BASE}/api/v3/depth"
    params = {"symbol": symbol.upper(), "limit": depth}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            raw = r.json()
        return {
            "bids": [[safe_float(p), safe_float(q)] for p, q in raw["bids"]],
            "asks": [[safe_float(p), safe_float(q)] for p, q in raw["asks"]],
        }
    except Exception:
        return {"bids": [], "asks": []}


async def get_recent_trades(symbol: str, limit: int = 50) -> list[dict]:
    url    = f"{BASE}/api/v3/trades"
    params = {"symbol": symbol.upper(), "limit": limit}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
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
    except Exception:
        return []
