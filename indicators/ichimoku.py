"""Ichimoku Cloud — Phase 3+ usage."""


def _midpoint(highs: list[float], lows: list[float], period: int) -> float:
    if len(highs) < period:
        return 0.0
    h = max(highs[-period:])
    l = min(lows[-period:])
    return (h + l) / 2


def ichimoku(candles: list[dict]) -> dict:
    """
    Returns key Ichimoku values for the most recent candle.
    Requires at least 52 candles for full cloud calculation.
    """
    highs  = [c["high"]  for c in candles]
    lows   = [c["low"]   for c in candles]
    closes = [c["close"] for c in candles]

    tenkan = _midpoint(highs, lows, 9)
    kijun  = _midpoint(highs, lows, 26)
    span_a = (tenkan + kijun) / 2
    span_b = _midpoint(highs, lows, 52)

    current = closes[-1] if closes else 0.0

    above_cloud = current > max(span_a, span_b)
    below_cloud = current < min(span_a, span_b)

    direction = "neutral"
    if above_cloud and tenkan > kijun:
        direction = "bullish"
    elif below_cloud and tenkan < kijun:
        direction = "bearish"

    return {
        "tenkan":      tenkan,
        "kijun":       kijun,
        "span_a":      span_a,
        "span_b":      span_b,
        "above_cloud": above_cloud,
        "below_cloud": below_cloud,
        "direction":   direction,
    }
