"""
News Feed Service — CryptoPanic public API.
Falls back to empty list if unavailable.
"""

import httpx
from utils.formatters import utc_now_iso

CRYPTOPANIC_BASE = "https://cryptopanic.com/api/v1/posts"
TIMEOUT          = 10


async def get_news(filter_: str = "hot", currencies: str | None = None) -> list[dict]:
    params: dict = {"public": "true", "filter": filter_}
    if currencies:
        params["currencies"] = currencies

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            r = await client.get(CRYPTOPANIC_BASE, params=params)
            r.raise_for_status()
            raw = r.json()

        results = []
        for item in raw.get("results", [])[:20]:
            results.append({
                "id":         item.get("id"),
                "title":      item.get("title", ""),
                "url":        item.get("url", ""),
                "source":     item.get("source", {}).get("title", "Unknown"),
                "published":  item.get("published_at", utc_now_iso()),
                "sentiment":  _parse_sentiment(item),
                "currencies": [c["code"] for c in item.get("currencies", [])],
            })
        return results

    except Exception:
        return []


def _parse_sentiment(item: dict) -> str:
    votes   = item.get("votes", {})
    bullish = votes.get("positive", 0)
    bearish = votes.get("negative", 0)
    if bullish > bearish:
        return "bullish"
    if bearish > bullish:
        return "bearish"
    return "neutral"
