"""MACD — Moving Average Convergence Divergence."""

from .ema import ema_series


def macd(prices: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
    """
    Returns:
        macd_line:   MACD line value (most recent)
        signal_line: Signal line value (most recent)
        histogram:   Histogram value (most recent)
        direction:   "bullish" | "bearish" | "neutral"
    """
    if len(prices) < slow + signal:
        return {"macd_line": 0.0, "signal_line": 0.0, "histogram": 0.0, "direction": "neutral"}

    fast_ema  = ema_series(prices, fast)
    slow_ema  = ema_series(prices, slow)
    macd_line = [f - s for f, s in zip(fast_ema, slow_ema)]
    sig_line  = ema_series(macd_line, signal)
    histogram = macd_line[-1] - sig_line[-1]

    direction = "neutral"
    if macd_line[-1] > sig_line[-1] and histogram > 0:
        direction = "bullish"
    elif macd_line[-1] < sig_line[-1] and histogram < 0:
        direction = "bearish"

    return {
        "macd_line":   macd_line[-1],
        "signal_line": sig_line[-1],
        "histogram":   histogram,
        "direction":   direction,
    }
