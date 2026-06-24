"""Momentum Engine — RSI + Rate of Change."""

from indicators.rsi import rsi, rsi_zone
from models.signal import MomentumResult


def analyse_momentum(closes: list[float]) -> MomentumResult:
    rsi_val = rsi(closes, period=14)
    zone    = rsi_zone(rsi_val)

    if zone == "oversold":
        direction = "bullish"
    elif zone == "overbought":
        direction = "bearish"
    elif rsi_val > 55:
        direction = "bullish"
    elif rsi_val < 45:
        direction = "bearish"
    else:
        direction = "neutral"

    return MomentumResult(
        rsi=round(rsi_val, 2),
        rsi_zone=zone,
        direction=direction,
    )
