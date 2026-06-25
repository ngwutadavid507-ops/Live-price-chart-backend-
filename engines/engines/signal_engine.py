"""
Signal Engine — the core firing mechanism.
Fires ONLY when 3+ indicators agree AND confidence > 68%.
"""

from indicators.macd import macd as calc_macd
from models.signal import SignalResult, TrendResult, MomentumResult, VolatilityResult, VolumeResult
from config import cfg


def generate_signal(
    symbol:     str,
    closes:     list[float],
    candles:    list[dict],
    trend:      TrendResult,
    momentum:   MomentumResult,
    volatility: VolatilityResult,
    volume:     VolumeResult,
) -> SignalResult:
    votes: list[str] = []

    votes.append(trend.direction)
    votes.append(momentum.direction)

    m = calc_macd(closes)
    votes.append(m["direction"])

    votes.append(volatility.direction)
    votes.append(volume.direction)

    if "strong" in trend.strength:
        votes.append(trend.direction)

    non_neutral   = [v for v in votes if v != "neutral"]
    if not non_neutral:
        return _no_signal(symbol)

    bullish_count = non_neutral.count("bullish")
    bearish_count = non_neutral.count("bearish")

    if bullish_count > bearish_count:
        consensus  = "bullish"
        agreements = bullish_count
    elif bearish_count > bullish_count:
        consensus  = "bearish"
        agreements = bearish_count
    else:
        return _no_signal(symbol)

    confidence = (agreements / len(votes)) * 100

    fired = (
        agreements >= cfg.SIGNAL_MIN_AGREEMENTS
        and confidence > cfg.SIGNAL_MIN_CONFIDENCE
    )

    direction   = "buy" if consensus == "bullish" else "sell"
    price       = closes[-1] if closes else 0.0
    atr_val     = candles[-1].get("atr", price * 0.01) if candles else price * 0.01

    entry       = round(price, 6)                        if fired else None
    stop_loss   = round(price - atr_val * 1.5, 6)       if fired and direction == "buy"  else \
                  round(price + atr_val * 1.5, 6)       if fired and direction == "sell" else None
    take_profit = round(price + atr_val * 3.0, 6)       if fired and direction == "buy"  else \
                  round(price - atr_val * 3.0, 6)       if fired and direction == "sell" else None

    return SignalResult(
        symbol=symbol,
        direction=direction if fired else "neutral",
        confidence=round(confidence, 2),
        agreements=agreements,
        entry=entry,
        stop_loss=stop_loss,
        take_profit=take_profit,
        fired=fired,
    )


def _no_signal(symbol: str) -> SignalResult:
    return SignalResult(
        symbol=symbol, direction="neutral",
        confidence=0.0, agreements=0, fired=False,
  )
