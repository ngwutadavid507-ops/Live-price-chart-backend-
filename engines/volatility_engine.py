"""
Volatility Engine — ATR (full rewrite) + Bollinger Bands.
"""

from indicators.atr import atr, atr_pct
from indicators.bollinger import bollinger_bands
from models.signal import VolatilityResult


def analyse_volatility(candles: list[dict]) -> VolatilityResult:
    """
    Requires candle list with: open, high, low, close, volume.
    """
    closes = [c["close"] for c in candles]

    atr_val = atr(candles, period=14)
    atr_p   = atr_pct(candles, period=14)
    bb      = bollinger_bands(closes, period=20, multiplier=2.0)

    direction = bb["direction"]

    return VolatilityResult(
        atr=round(atr_val, 6),
        atr_pct=round(atr_p, 4),
        bb_upper=round(bb["upper"], 6),
        bb_middle=round(bb["middle"], 6),
        bb_lower=round(bb["lower"], 6),
        bb_squeeze=bb["squeeze"],
        direction=direction,
    )
