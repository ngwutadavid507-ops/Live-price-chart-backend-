from pydantic import BaseModel
from typing import Optional


class TrendResult(BaseModel):
    direction:   str
    strength:    str
    ema9:        float
    ema21:       float
    ema50:       float
    ema_aligned: bool
    ema_spread:  float


class MomentumResult(BaseModel):
    rsi:          float
    rsi_zone:     str
    direction:    str


class VolatilityResult(BaseModel):
    atr:          float
    atr_pct:      float
    bb_upper:     float
    bb_middle:    float
    bb_lower:     float
    bb_squeeze:   bool
    direction:    str


class VolumeResult(BaseModel):
    volume_trend: str
    spike:        bool
    spike_ratio:  float
    direction:    str


class SignalResult(BaseModel):
    symbol:       str
    direction:    str
    confidence:   float
    agreements:   int
    entry:        Optional[float] = None
    stop_loss:    Optional[float] = None
    take_profit:  Optional[float] = None
    fired:        bool


class AnalysisResult(BaseModel):
    symbol:        str
    trend:         TrendResult
    momentum:      MomentumResult
    volatility:    VolatilityResult
    volume:        VolumeResult
    signal:        SignalResult
    market_score:  float
    timestamp:     int
