"""
Analysis Cache — stores computed analysis results per symbol.
TTL: 60 seconds. Only analyses hot symbols proactively.
"""

import asyncio
import time
from config import cfg
from cache.market_cache import market_cache
from services.binance import get_klines
from engines import (
    analyse_trend, analyse_momentum, analyse_volatility,
    analyse_volume, generate_signal, compute_market_score,
)
from models.signal import AnalysisResult
from utils.formatters import timestamp_ms


class AnalysisCache:
    def __init__(self):
        self._data:     dict[str, AnalysisResult] = {}
        self._built_at: float = 0.0
        self._ready:    bool  = False

    def get(self, symbol: str) -> AnalysisResult | None:
        return self._data.get(symbol.upper())

    def all(self) -> list[AnalysisResult]:
        return list(self._data.values())

    def is_ready(self) -> bool:
        return self._ready

    def meta(self) -> dict:
        return {
            "status":         "ready" if self._ready else "building",
            "analysed_count": len(self._data),
            "age_seconds":    round(time.time() - self._built_at, 1),
        }

    async def get_or_compute(self, symbol: str) -> AnalysisResult | None:
        cached = self._data.get(symbol.upper())
        if cached:
            return cached
        return await self._analyse_symbol(symbol.upper())

    async def build(self):
        hot     = market_cache.hot()
        symbols = [h["symbol"] for h in hot]
        tasks   = [self._analyse_symbol(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, result in zip(symbols, results):
            if isinstance(result, AnalysisResult):
                self._data[sym] = result
        self._built_at = time.time()
        self._ready    = True

    async def auto_refresh(self):
        while True:
            await asyncio.sleep(cfg.ANALYSIS_CACHE_TTL)
            try:
                await self.build()
            except Exception:
                pass

    async def _analyse_symbol(self, symbol: str) -> AnalysisResult | None:
        try:
            candles = await get_klines(symbol, interval="1h", limit=100)
            if len(candles) < 20:
                return None

            closes     = [c["close"] for c in candles]
            trend      = analyse_trend(closes)
            momentum   = analyse_momentum(closes)
            volatility = analyse_volatility(candles)
            volume     = analyse_volume(candles)
            signal     = generate_signal(symbol, closes, candles, trend, momentum, volatility, volume)
            score      = compute_market_score(trend, momentum, volatility, volume)

            result = AnalysisResult(
                symbol=symbol,
                trend=trend,
                momentum=momentum,
                volatility=volatility,
                volume=volume,
                signal=signal,
                market_score=score,
                timestamp=timestamp_ms(),
            )
            self._data[symbol] = result
            return result
        except Exception:
            return None


analysis_cache = AnalysisCache()
