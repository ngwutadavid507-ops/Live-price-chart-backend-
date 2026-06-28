"""
Analysis Cache — CoinGecko OHLC with API key for candles.
Live prices from Bybit/OKX/BingX WebSockets.
"""

import asyncio
import time
import httpx
from config import cfg
from cache.market_cache import market_cache
from utils.safe_float import safe_float
from engines import (
    analyse_trend, analyse_momentum, analyse_volatility,
    analyse_volume, generate_signal, compute_market_score,
)
from models.signal import AnalysisResult
from utils.formatters import timestamp_ms

SYMBOL_TO_CG_ID = {
    "BTCUSDT":   "bitcoin",
    "ETHUSDT":   "ethereum",
    "SOLUSDT":   "solana",
    "BNBUSDT":   "binancecoin",
    "XRPUSDT":   "ripple",
    "ADAUSDT":   "cardano",
    "DOGEUSDT":  "dogecoin",
    "AVAXUSDT":  "avalanche-2",
    "DOTUSDT":   "polkadot",
    "LINKUSDT":  "chainlink",
    "UNIUSDT":   "uniswap",
    "LTCUSDT":   "litecoin",
    "ATOMUSDT":  "cosmos",
    "NEARUSDT":  "near",
    "MATICUSDT": "matic-network",
    "APTUSDT":   "aptos",
    "ARBUSDT":   "arbitrum",
    "OPUSDT":    "optimism",
    "SUIUSDT":   "sui",
    "PEPEUSDT":  "pepe",
}


def _cg_headers() -> dict:
    """CoinGecko Demo API headers."""
    headers = {"accept": "application/json"}
    if cfg.COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = cfg.COINGECKO_API_KEY
    return headers


async def _get_candles(symbol: str) -> list[dict]:
    """Fetch OHLC from CoinGecko with API key."""
    coin_id = SYMBOL_TO_CG_ID.get(symbol.upper())
    if not coin_id:
        return []

    url     = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
    headers = _cg_headers()

    for days in [1, 7]:
        try:
            params = {"vs_currency": "usd", "days": str(days)}
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(url, params=params, headers=headers)
            if r.status_code == 200:
                raw = r.json()
                if raw and len(raw) >= 10:
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
            elif r.status_code == 429:
                await asyncio.sleep(5)
                continue
        except Exception:
            continue
    return []


async def _build_from_price(symbol: str) -> AnalysisResult | None:
    """Fallback: build basic analysis from live WebSocket price."""
    asset = market_cache.get(symbol)
    if not asset or asset["price"] <= 0:
        return None

    price = asset["price"]
    high  = asset.get("high", price * 1.02)
    low   = asset.get("low",  price * 0.98)

    candles = [
        {"time": i, "open": low, "high": high,
         "low": low, "close": price, "volume": 1000}
        for i in range(20)
    ]
    closes = [price] * 20

    try:
        trend      = analyse_trend(closes)
        momentum   = analyse_momentum(closes)
        volatility = analyse_volatility(candles)
        volume     = analyse_volume(candles)
        signal     = generate_signal(
            symbol, closes, candles,
            trend, momentum, volatility, volume
        )
        score = compute_market_score(trend, momentum, volatility, volume)
        return AnalysisResult(
            symbol=symbol,
            trend=trend,
            momentum=momentum,
            volatility=volatility,
            volume=volume,
            signal=signal,
            market_score=score,
            timestamp=timestamp_ms(),
        )
    except Exception:
        return None


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
        symbols = list(SYMBOL_TO_CG_ID.keys())
        for i in range(0, len(symbols), 3):
            batch   = symbols[i:i+3]
            tasks   = [self._analyse_symbol(s) for s in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, result in zip(batch, results):
                if isinstance(result, AnalysisResult):
                    self._data[sym] = result
            await asyncio.sleep(1)
        self._built_at = time.time()
        self._ready    = True
        print(f"Analysis cache ready — {len(self._data)} symbols")

    async def auto_refresh(self):
        while True:
            await asyncio.sleep(cfg.ANALYSIS_CACHE_TTL)
            try:
                await self.build()
            except Exception:
                pass

    async def _analyse_symbol(self, symbol: str) -> AnalysisResult | None:
        candles = await _get_candles(symbol)
        if len(candles) >= 10:
            closes = [c["close"] for c in candles]
            try:
                trend      = analyse_trend(closes)
                momentum   = analyse_momentum(closes)
                volatility = analyse_volatility(candles)
                volume     = analyse_volume(candles)
                signal     = generate_signal(
                    symbol, closes, candles,
                    trend, momentum, volatility, volume
                )
                score = compute_market_score(trend, momentum, volatility, volume)
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
                pass

        # Fallback to live price
        return await _build_from_price(symbol)


analysis_cache = AnalysisCache()
