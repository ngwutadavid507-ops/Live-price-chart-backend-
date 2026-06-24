"""EMA — Exponential Moving Average."""


def ema(prices: list[float], period: int) -> float:
    """
    Calculate EMA for the given price series.
    Returns the final EMA value (most recent).
    """
    if not prices or len(prices) < period:
        return prices[-1] if prices else 0.0
    k = 2 / (period + 1)
    result = prices[0]
    for price in prices[1:]:
        result = price * k + result * (1 - k)
    return result


def ema_series(prices: list[float], period: int) -> list[float]:
    """Return the full EMA series (same length as prices)."""
    if not prices:
        return []
    k = 2 / (period + 1)
    result = [prices[0]]
    for price in prices[1:]:
        result.append(price * k + result[-1] * (1 - k))
    return result
