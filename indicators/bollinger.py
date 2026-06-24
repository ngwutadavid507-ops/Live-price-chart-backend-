"""Bollinger Bands."""

import math


def bollinger_bands(prices: list[float], period: int = 20, multiplier: float = 2.0) -> dict:
    """
    Returns upper, middle, lower bands and squeeze detection.
    """
    if len(prices) < period:
        mid = prices[-1] if prices else 0.0
        return {
            "upper":     mid,
            "middle":    mid,
            "lower":     mid,
            "bandwidth": 0.0,
            "squeeze":   False,
            "direction": "neutral",
        }

    slice_   = prices[-period:]
    middle   = sum(slice_) / period
    variance = sum((p - middle) ** 2 for p in slice_) / period
    std_dev  = math.sqrt(variance)

    upper     = middle + multiplier * std_dev
    lower     = middle - multiplier * std_dev
    bandwidth = (upper - lower) / middle * 100 if middle else 0.0

    current   = prices[-1]
    direction = "neutral"
    if current > upper:
        direction = "bearish"
    elif current < lower:
        direction = "bullish"

    return {
        "upper":     upper,
        "middle":    middle,
        "lower":     lower,
        "bandwidth": bandwidth,
        "squeeze":   bandwidth < 2.0,
        "direction": direction,
    }
