"""
ATR — Average True Range.
Full rewrite. Clean implementation using Wilder's smoothing.
"""


def true_range(high: float, low: float, prev_close: float) -> float:
    """Single True Range value."""
    return max(
        high - low,
        abs(high - prev_close),
        abs(low  - prev_close),
    )


def atr(candles: list[dict], period: int = 14) -> float:
    """
    Wilder's ATR from candle list.
    Each candle must have: high, low, close.
    Returns ATR value. Returns 0.0 if insufficient data.
    """
    if len(candles) < period + 1:
        return 0.0

    trs = []
    for i in range(1, len(candles)):
        tr = true_range(
            candles[i]["high"],
            candles[i]["low"],
            candles[i - 1]["close"],
        )
        trs.append(tr)

    if len(trs) < period:
        return 0.0

    current_atr = sum(trs[:period]) / period

    for tr in trs[period:]:
        current_atr = (current_atr * (period - 1) + tr) / period

    return current_atr


def atr_pct(candles: list[dict], period: int = 14) -> float:
    """ATR as percentage of current close price."""
    if not candles:
        return 0.0
    atr_val = atr(candles, period)
    close   = candles[-1]["close"]
    if close == 0:
        return 0.0
    return (atr_val / close) * 100
