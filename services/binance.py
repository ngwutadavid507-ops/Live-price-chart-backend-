"""
Binance Service — uses api1.binance.com which works on cloud/Render IPs.
api.binance.com gets blocked on shared cloud IPs.
api1/api2/api3 are alternative endpoints that bypass this.
"""

import httpx
from utils.safe_float import safe_float

BASES = [
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]
TIMEOUT = 15

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


async def _get(path: str, params: dict = {}) -> dict | list | None:
    """Try each Binance base URL until one works."""
    for base in BASES:
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
                r = await client.get(f"{base}{path}", params=params)
                if r.status_code == 200:
                    return r.json()
        except Exception:
            continue
    return None


async def get_ticker_24h(symbol: str | None = None) -> list[dict]:
    params = {"symbol": symbol.upper()} if symbol else {}
    data   = await _get("/api/v3/ticker/24hr", params)
    if not data:
        return []
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
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    raw    = await _get("/api/v3/klines", params)
    if not raw:
        return []
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
    params = {"symbol": symbol.upper(), "limit": depth}
    raw    = await _get("/api/v3/depth", params)
    if not raw:
        return {"bids": [], "asks": []}
    return {
        "bids": [[safe_float(p), safe_float(q)] for p, q in raw["bids"]],
        "asks": [[safe_float(p), safe_float(q)] for p, q in raw["asks"]],
    }


async def get_recent_trades(symbol: str, limit: int = 50) -> list[dict]:
    params = {"symbol": symbol.upper(), "limit": limit}
    raw    = await _get("/api/v3/trades", params)
    if not raw:
        return []
    return [
        {
            "price":    safe_float(t["price"]),
            "qty":      safe_float(t["qty"]),
            "is_buyer": t["isBuyerMaker"],
            "time":     int(t["time"]),
        }
        for t in raw
    ]
