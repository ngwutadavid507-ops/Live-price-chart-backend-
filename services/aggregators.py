"""
Aggregator Services — CoinGecko and CoinPaprika.
Used ONLY as fallback when all exchange sources fail.
"""

import httpx
from utils.safe_float import safe_float

COINGECKO_BASE   = "https://api.coingecko.com/api/v3"
COINPAPRIKA_BASE = "https://api.coinpaprika.com/v1"
TIMEOUT          = 15


async def cg_get_markets(per_page: int = 250, page: int = 1) -> list[dict]:
    url    = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency":             "usd",
        "order":                   "market_cap_desc",
        "per_page":                per_page,
        "page":                    page,
        "sparkline":               True,
        "price_change_percentage": "24h",
    }
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        raw = r.json()

    results = []
    for c in raw:
        sparkline = c.get("sparkline_in_7d", {}).get("price", [])
        spark5    = sparkline[-5:] if len(sparkline) >= 5 else sparkline
        results.append({
            "symbol":     c["symbol"].upper() + "USDT",
            "name":       c["name"],
            "price":      safe_float(c.get("current_price")),
            "change24h":  safe_float(c.get("price_change_percentage_24h")),
            "volume":     safe_float(c.get("total_volume")),
            "high":       safe_float(c.get("high_24h")),
            "low":        safe_float(c.get("low_24h")),
            "market_cap": safe_float(c.get("market_cap")),
            "sparkline":  spark5,
            "source":     "coingecko",
        })
    return results


async def cg_get_ohlc(coin_id: str, days: int = 1) -> list[dict]:
    url    = f"{COINGECKO_BASE}/coins/{coin_id}/ohlc"
    params = {"vs_currency": "usd", "days": days}
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
            "volume": 0.0,
        }
        for k in raw
    ]


async def cp_get_tickers() -> list[dict]:
    url = f"{COINPAPRIKA_BASE}/tickers"
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.get(url)
        r.raise_for_status()
        raw = r.json()
    results = []
    for c in raw[:200]:
        q = c.get("quotes", {}).get("USD", {})
        results.append({
            "symbol":     c["symbol"].upper() + "USDT",
            "name":       c["name"],
            "price":      safe_float(q.get("price")),
            "change24h":  safe_float(q.get("percent_change_24h")),
            "volume":     safe_float(q.get("volume_24h")),
            "high":       0.0,
            "low":        0.0,
            "market_cap": safe_float(q.get("market_cap")),
            "sparkline":  [],
            "source":     "coinpaprika",
        })
    return results
