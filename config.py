"""
Phoenix Backend Config
All env vars loaded once here. Import `cfg` anywhere you need them.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Exchange Keys
    BYBIT_API_KEY:    str = os.getenv("BYBIT_API_KEY", "")
    BYBIT_API_SECRET: str = os.getenv("BYBIT_API_SECRET", "")

    # AI
    GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
    GROQ_MODEL:   str = "llama-3.3-70b-versatile"

    # CoinGecko
    COINGECKO_API_KEY: str = os.getenv("COINGECKO_API_KEY", "")

    # Cache TTLs (seconds)
    MARKET_CACHE_TTL:   int = 10
    ANALYSIS_CACHE_TTL: int = 60
    CANDLE_CACHE_TTL:   int = 30

    # Signal Engine
    SIGNAL_MIN_AGREEMENTS: int   = 3
    SIGNAL_MIN_CONFIDENCE: float = 68.0

    # Data source priority
    EXCHANGE_PRIORITY:    list = ["bybit", "okx", "bingx"]
    FALLBACK_AGGREGATORS: list = ["coingecko"]

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = int(os.getenv("PORT", 8000))


cfg = Config()
