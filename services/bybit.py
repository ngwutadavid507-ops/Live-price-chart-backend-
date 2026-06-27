import httpx
from utils.safe_float import safe_float

BASE = "https://api.bybit.com"
TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


async def _get(path: str, params: dict | None = None):
    try:
        async with httpx.AsyncClient(
            timeout=TIMEOUT,
            headers=HEADERS,
            follow_redirects=True,
        ) as client:
            r = await client.get(
                f"{BASE}{path}",
                params=params or {},
            )

            if r.status_code != 200:
                print(
                    f"BYBIT ERROR {r.status_code}: "
                    f"{r.text[:500]}"
                )
                return None

            return r.json()

    except Exception as e:
        print(f"BYBIT EXCEPTION: {e}")
        return None


async def get_tickers(category: str = "linear") -> list[dict]:
    raw = await _get(
        "/v5/market/tickers",
        {"category": category},
    )

    if not raw:
        return []

    results = []

    for t in raw.get("result", {}).get("list", []):
        price = safe_float(t.get("lastPrice"))

        if price <= 0:
            continue

        results.append(
            {
                "symbol": t.get("symbol", ""),
                "price": price,
                "change24h": safe_float(
                    t.get("price24hPcnt")
                ) * 100,
                "volume": safe_float(
                    t.get("turnover24h")
                ),
                "high": safe_float(
                    t.get("highPrice24h")
                ),
                "low": safe_float(
                    t.get("lowPrice24h")
                ),
                "open_interest": safe_float(
                    t.get("openInterest")
                ),
                "funding_rate": safe_float(
                    t.get("fundingRate")
                ),
                "source": "bybit",
            }
        )

    return results


async def get_instruments(
    category: str = "linear",
) -> set[str]:
    raw = await _get(
        "/v5/market/instruments-info",
        {
            "category": category,
            "status": "Trading",
        },
    )

    if not raw:
        return set()

    items = raw.get("result", {}).get("list", [])

    return {
        item["symbol"]
        for item in items
        if item.get("status") == "Trading"
    }


async def get_order_book(
    symbol: str,
    category: str = "linear",
    depth: int = 25,
) -> dict:

    raw = await _get(
        "/v5/market/orderbook",
        {
            "category": category,
            "symbol": symbol.upper(),
            "limit": depth,
        },
    )

    if not raw:
        return {
            "bids": [],
            "asks": [],
        }

    book = raw.get("result", {})

    return {
        "bids": [
            [safe_float(p), safe_float(q)]
            for p, q in book.get("b", [])
        ],
        "asks": [
            [safe_float(p), safe_float(q)]
            for p, q in book.get("a", [])
        ],
    }


async def get_klines(
    symbol: str,
    interval: str = "60",
    limit: int = 100,
) -> list[dict]:

    raw = await _get(
        "/v5/market/kline",
        {
            "category": "linear",
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        },
    )

    if not raw:
        return []

    items = raw.get("result", {}).get("list", [])

    return [
        {
            "time": int(k[0]),
            "open": safe_float(k[1]),
            "high": safe_float(k[2]),
            "low": safe_float(k[3]),
            "close": safe_float(k[4]),
            "volume": safe_float(k[5]),
        }
        for k in reversed(items)
    ]
