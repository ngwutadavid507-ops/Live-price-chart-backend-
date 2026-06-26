"""
Bybit Service — live pairs and futures data.
"""

import httpx
from utils.safe_float import safe_float

BASE    = "https://api.bybit.com"
TIMEOUT = 10


async def get_tickers(category: str = "linear") -> list[dict]:
    url    = f"{BASE}/v5/market/tickers"
    params = {"category": category}
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        raw = r.json()

    results = []
    items   = raw.get("result", {}).get("list", [])
    for t in items:
        price = safe_float(t.get("lastPrice"))
        if price == 0.0:
            continue
        results.append({
            "symbol":        t.get("symbol", ""),
            "price":         price,
            "change24h":     safe_float(t.get("price24hPcnt", 0)) * 100,
            "volume":        safe_float(t.get("turnover24h")),
            "high":          safe_float(t.get("highPrice24h")),
            "low":           safe_float(t.get("lowPrice24h")),
            "open_interest": safe_float(t.get("openInterest")),
            "funding_rate":  safe_float(t.get("fundingRate")),
            "source":        "bybit",
        })
    return results


async def get_instruments(category: str = "linear") -> set[str]:
    url    = f"{BASE}/v5/market/instruments-info"
    params = {"category": category, "status": "Trading"}
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        raw = r.json()
    items = raw.get("result", {}).get("list", [])
    return {i["symbol"] for i in items if i.get("status") == "Trading"}


async def get_order_book(symbol: str, category: str = "linear", depth: int = 25) -> dict:
    url    = f"{BASE}/v5/market/orderbook"
    params = {"category": category, "symbol": symbol.upper(), "limit": depth}
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        raw = r.json()
    book = raw.get("result", {})
    return {
        "bids": [[safe_float(p), safe_float(q)] for p, q in book.get("b", [])],
        "asks": [[safe_float(p), safe_float(q)] for p, q in book.get("a", [])],
    }


async def get_klines(symbol: str, interval: str = "60", limit: int = 100) -> list[dict]:
    url = f"{BASE}/v5/market/kline"
    params = {
        "category": "linear",
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit,
    }
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
