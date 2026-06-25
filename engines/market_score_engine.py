"""
Market Score Engine — Phase 2.
Produces a 0-100 composite score from all sub-engines.
"""

from models.signal import TrendResult, MomentumResult, VolatilityResult, VolumeResult

WEIGHTS = {
    "trend":      0.35,
    "momentum":   0.25,
    "volatility": 0.20,
    "volume":     0.20,
}


def compute_market_score(
    trend:      TrendResult,
    momentum:   MomentumResult,
    volatility: VolatilityResult,
    volume:     VolumeResult,
) -> float:
    trend_score      = _score_trend(trend)
    momentum_score   = _score_momentum(momentum)
    volatility_score = _score_volatility(volatility)
    volume_score     = _score_volume(volume)

    score = (
        trend_score      * WEIGHTS["trend"]      +
        momentum_score   * WEIGHTS["momentum"]   +
        volatility_score * WEIGHTS["volatility"] +
        volume_score     * WEIGHTS["volume"]
    )
    return round(score, 2)


def _score_trend(t: TrendResult) -> float:
    mapping = {
        "strong_bullish": 90,
        "weak_bullish":   65,
        "neutral":        50,
        "weak_bearish":   35,
        "strong_bearish": 10,
    }
    return mapping.get(t.strength, 50)


def _score_momentum(m: MomentumResult) -> float:
    rsi = m.rsi
    if rsi <= 30:
        return 75
    if rsi <= 45:
        return 40
    if rsi <= 55:
        return 50
    if rsi <= 65:
        return 65
    if rsi <= 70:
        return 55
    return 30


def _score_volatility(v: VolatilityResult) -> float:
    if v.bb_squeeze:
        return 55
    if v.direction == "bullish":
        return 70
    if v.direction == "bearish":
        return 30
    return 50


def _score_volume(v: VolumeResult) -> float:
    if v.spike and v.direction == "bullish":
        return 85
    if v.spike and v.direction == "bearish":
        return 15
    if v.volume_trend == "rising":
        return 65
    if v.volume_trend == "falling":
        return 35
    return 50
