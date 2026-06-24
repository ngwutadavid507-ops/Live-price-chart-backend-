"""
Trend Engine — Phase 2 upgrade.
EMA alignment + spread scoring. Returns strength categories.
"""

from indicators.ema import ema
from models.signal import TrendResult


def analyse_trend(closes: list[float]) -> TrendResult:
    """
    Compute EMA9, EMA21, EMA50 and determine trend direction + strength.

    Strength labels:
        strong_bullish  — ema9 > ema21 > ema50, good spread
        weak_bullish    — ema9 > ema21 but not fully aligned
        strong_bearish  — ema9 < ema21 < ema50, good spread
        weak_bearish    — ema9 < ema21 but not fully aligned
        neutral         — mixed / insufficient data
    """
    if len(closes) < 50:
        return _neutral(closes)

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)

    bull_aligned = e9 > e21 > e50
    bear_aligned = e9 < e21 < e50

    price  = closes[-1]
    spread = abs(e9 - e50) / price * 100 if price else 0.0
    SPREAD_THRESHOLD = 0.5

    if bull_aligned:
        direction = "bullish"
        strength  = "strong_bullish" if spread >= SPREAD_THRESHOLD else "weak_bullish"
    elif bear_aligned:
        direction = "bearish"
        strength  = "strong_bearish" if spread >= SPREAD_THRESHOLD else "weak_bearish"
    elif e9 > e21:
        direction = "bullish"
        strength  = "weak_bullish"
    elif e9 < e21:
        direction = "bearish"
        strength  = "weak_bearish"
    else:
        direction = "neutral"
        strength  = "neutral"

    return TrendResult(
        direction=direction,
        strength=strength,
        ema9=round(e9, 6),
        ema21=round(e21, 6),
        ema50=round(e50, 6),
        ema_aligned=bull_aligned or bear_aligned,
        ema_spread=round(spread, 4),
    )


def _neutral(closes: list[float]) -> TrendResult:
    last = closes[-1] if closes else 0.0
    return TrendResult(
        direction="neutral",
        strength="neutral",
        ema9=last, ema21=last, ema50=last,
        ema_aligned=False,
        ema_spread=0.0,
    )
