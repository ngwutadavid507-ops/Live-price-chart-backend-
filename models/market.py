from pydantic import BaseModel
from typing import Optional


class AssetPrice(BaseModel):
    symbol:      str
    name:        str
    price:       float
    change24h:   float
    volume:      str
    volume_raw:  float
    high:        float
    low:         float
    market_cap:  Optional[float] = None
    sparkline:   list[float]  = []
    source:      str
    confidence:  str


class TickerSummary(BaseModel):
    symbol:    str
    price:     float
    change24h: float


class CandleBar(BaseModel):
    time:   int
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float


class MarketSummary(BaseModel):
    bullish_count:     int
    bearish_count:     int
    neutral_count:     int
    avg_market_score:  float
    strongest_bullish: list[str]
    strongest_bearish: list[str]
    total_tracked:     int
