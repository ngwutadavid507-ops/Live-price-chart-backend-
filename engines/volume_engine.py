"""Volume Engine — trend detection and spike detection."""

from models.signal import VolumeResult


def analyse_volume(candles: list[dict], spike_multiplier: float = 2.0) -> VolumeResult:
    """
    Analyses recent volume trend and detects spikes.
    spike_multiplier: volume must be N× the 20-period average to count as a spike.
    """
    if len(candles) < 5:
        return VolumeResult(
            volume_trend="flat", spike=False, spike_ratio=1.0, direction="neutral"
        )

    volumes = [c["volume"] for c in candles]
    recent  = volumes[-3:]
    older   = volumes[-10:-3] if len(volumes) >= 10 else volumes[:-3]

    avg_recent = sum(recent) / len(recent) if recent else 0
    avg_older  = sum(older)  / len(older)  if older  else 0

    if avg_older == 0:
        trend = "flat"
    elif avg_recent > avg_older * 1.2:
        trend = "rising"
    elif avg_recent < avg_older * 0.8:
        trend = "falling"
    else:
        trend = "flat"

    period_vols = volumes[-20:]
    avg_20  = sum(period_vols) / len(period_vols) if period_vols else 1
    latest  = volumes[-1]
    ratio   = latest / avg_20 if avg_20 else 1.0
    spike   = ratio >= spike_multiplier

    direction = "neutral"
    if trend == "rising" or spike:
        last = candles[-1]
        direction = "bullish" if last["close"] >= last["open"] else "bearish"

    return VolumeResult(
        volume_trend=trend,
        spike=spike,
        spike_ratio=round(ratio, 2),
        direction=direction,
    )
