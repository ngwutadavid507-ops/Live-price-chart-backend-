"""
Market Cache — stores live prices for all symbols.
TTL: 10 seconds. Source priority: binance → bybit → coingecko → coinpaprika.
"""

import asyncio
import time
from config import cfg
from services.binance import get_ticker_24h
from services.bybit import get_tickers as bybit_tickers
from services.aggregators import cg_get_markets, cp_get_tickers
from utils.formatters import fmt_compact
from utils.safe_float import safe_float


class MarketCache:
    def __init__(self):
        self._data:       dict[str, dict] = {}
        self._hot:        list[dict]      = []
        self._source_log: dict[str, str]  = {}
        self._built_at:   float           = 0.0
        self._ready:      bool            = False

    def get(self, symbol: str) -> dict | None:
        return self._data.get(symbol.upper())

    def all(self) -> list[dict]:
        return list(self._data.values())

    def hot(self) -> list[dict]:
        return self._hot

    def is_ready(self) -> bool:
        return self._ready

    def meta(self) -> dict:
        exchange_sources   = [s for s in self._source_log.values() if s in {"binance","bybit","okx","mexc","bingx"}]
        aggregator_sources = [s for s in self._source_log.values() if s in {"coingecko","coinpaprika"}]
        return {
            "total_symbols":    len(self._data),
            "exchange_count":   len(exchange_sources),
            "aggregator_count": len(aggregator_sources),
            "source_log":       dict(list(self._source_log.items())[:10]),
            "age_seconds":      round(time.time() - self._built_at, 1),
        }

    async def build(self):
        merged: dict[str, dict] = {}

        try:
            tickers = await get_ticker_24h()
            for t in tickers:
                sym = t["symbol"]
                merged[sym] = self._enrich(t)
                self._source_log[sym] = "binance"
        except Exception:
            pass

        try:
            bybit = await bybit_tickers("linear")
            for t in bybit:
                sym = t["symbol"]
                if sym not in merged:
                    merged[sym] = self._enrich(t)
                    self._source_log[sym] = "bybit"
        except Exception:
            pass

        if len(merged) < 100:
            try:
                cg = await cg_get_markets(per_page=250)
                for t in cg:
                    sym = t["symbol"]
                    if sym not in merged:
                        merged[sym] = self._enrich(t)
                        self._source_log[sym] = "coingecko"
            except Exception:
                pass

        if len(merged) < 50:
            try:
                cp = await cp_get_tickers()
                for t in cp:
                    sym = t["symbol"]
                    if sym not in merged:
                        merged[sym] = self._enrich(t)
                        self._source_log[sym] = "coinpaprika"
            except Exception:
                pass

        self._data     = merged
        self._hot      = self._compute_hot()
        self._built_at = time.time()
        self._ready    = True

    async def auto_refresh(self):
        while True:
            await asyncio.sleep(cfg.MARKET_CACHE_TTL)
            try:
                await self.build()
            except Exception:
                pass

    def _enrich(self, t: dict) -> dict:
        vol_raw = safe_float(t.get("volume", 0))
        return {
            "symbol":     t.get("symbol", ""),
            "name":       t.get("name", t.get("symbol", "").replace("USDT", "")),
            "price":      safe_float(t.get("price", 0)),
            "change24h":  safe_float(t.get("change24h", 0)),
            "volume":     fmt_compact(vol_raw),
            "volume_raw": vol_raw,
            "high":       safe_float(t.get("high", 0)),
            "low":        safe_float(t.get("low", 0)),
            "market_cap": safe_float(t.get("market_cap", 0)),
            "sparkline":  t.get("sparkline", []),
            "source":     t.get("source", "unknown"),
            "confidence": "high" if t.get("source") in {"binance","bybit"} else "medium",
        }

    def _compute_hot(self) -> list[dict]:
        return sorted(
            self._data.values(),
            key=lambda x: x["volume_raw"],
            reverse=True,
        )[:20]


market_cache = MarketCache()
