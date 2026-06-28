"""
Market Cache — fed by Binance + Bybit WebSockets.
REST polling removed. WebSocket pushes prices in real time.
Works on Render free tier (WSS not blocked).
"""

import asyncio
import time
from utils.formatters import fmt_compact
from utils.safe_float import safe_float


class MarketCache:
    def __init__(self):
        self._data:       dict[str, dict] = {}
        self._built_at:   float           = 0.0
        self._ready:      bool            = False

    # ── Public API ────────────────────────────────────────────────────

    def get(self, symbol: str) -> dict | None:
        return self._data.get(symbol.upper())

    def all(self) -> list[dict]:
        return list(self._data.values())

    def hot(self) -> list[dict]:
        """Top 20 by volume."""
        return sorted(
            self._data.values(),
            key=lambda x: x.get("volume_raw", 0),
            reverse=True,
        )[:20]

    def is_ready(self) -> bool:
        return self._ready

    def meta(self) -> dict:
        sources = {}
        for v in self._data.values():
            src = v.get("source", "unknown")
            sources[src] = sources.get(src, 0) + 1
        return {
            "total_symbols":    len(self._data),
            "exchange_count":   sources.get("binance", 0) + sources.get("bybit", 0),
            "aggregator_count": sources.get("coingecko", 0) + sources.get("coinpaprika", 0),
            "source_log":       sources,
            "age_seconds":      round(time.time() - self._built_at, 1),
        }

    # ── WebSocket callback ────────────────────────────────────────────

    async def update_from_ws(
        self,
        symbol:    str,
        price:     float,
        change24h: float,
        volume:    float,
        high:      float,
        low:       float,
        source:    str,
    ):
        """Called by Binance/Bybit WebSocket on every tick."""
        sym = symbol.upper()
        existing = self._data.get(sym, {})

        self._data[sym] = {
            "symbol":     sym,
            "name":       existing.get("name", sym.replace("USDT", "")),
            "price":      price,
            "change24h":  change24h,
            "volume":     fmt_compact(volume),
            "volume_raw": volume,
            "high":       high,
            "low":        low,
            "market_cap": existing.get("market_cap", 0),
            "sparkline":  existing.get("sparkline", []),
            "source":     source,
            "confidence": "high",   # WebSocket = exchange data = high confidence
        }

        if not self._ready and len(self._data) >= 5:
            self._ready    = True
            self._built_at = time.time()

    # ── Startup seed (CoinGecko REST — one time only) ─────────────────

    async def build(self):
        """
        Seed initial data from CoinGecko.
        WebSocket will overwrite with real exchange prices immediately.
        """
        try:
            import httpx
            url    = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "order":       "market_cap_desc",
                "per_page":    50,
                "page":        1,
                "sparkline":   True,
            }
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(url, params=params)
                if r.status_code == 200:
                    raw = r.json()
                    for c in raw:
                        sym      = c["symbol"].upper() + "USDT"
                        vol_raw  = safe_float(c.get("total_volume", 0))
                        sparkline = c.get("sparkline_in_7d", {}).get("price", [])
                        self._data[sym] = {
                            "symbol":     sym,
                            "name":       c["name"],
                            "price":      safe_float(c.get("current_price", 0)),
                            "change24h":  safe_float(c.get("price_change_percentage_24h", 0)),
                            "volume":     fmt_compact(vol_raw),
                            "volume_raw": vol_raw,
                            "high":       safe_float(c.get("high_24h", 0)),
                            "low":        safe_float(c.get("low_24h", 0)),
                            "market_cap": safe_float(c.get("market_cap", 0)),
                            "sparkline":  sparkline[-5:] if len(sparkline) >= 5 else sparkline,
                            "source":     "coingecko_seed",
                            "confidence": "medium",
                        }
                    self._ready    = True
                    self._built_at = time.time()
                    print(f"Market cache seeded with {len(self._data)} assets")
        except Exception as e:
            print(f"Seed failed: {e} — waiting for WebSocket data")
            self._ready = True   # Don't block startup

    async def auto_refresh(self):
        """Not needed — WebSocket keeps data fresh. No-op."""
        pass


market_cache = MarketCache()
