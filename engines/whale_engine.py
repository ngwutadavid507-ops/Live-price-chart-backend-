"""
Whale Engine — Phase 3.
Detects large trades from public trade streams.
Threshold: trade value > $100K USD = whale.
"""

from utils.formatters import fmt_compact

WHALE_THRESHOLD_USD = 100_000


def detect_whales(trades: list[dict], price: float) -> list[dict]:
    """
    Filter trade list for whale-sized trades.
    trades: [{"price", "qty", "is_buyer", "time"}, ...]
    """
    alerts = []
    for t in trades:
        value = t["price"] * t["qty"]
        if value >= WHALE_THRESHOLD_USD:
            side = "Buy" if not t["is_buyer"] else "Sell"
            alerts.append({
                "side":      side,
                "qty":       round(t["qty"], 4),
                "value_usd": round(value, 2),
                "value_fmt": fmt_compact(value),
                "price":     t["price"],
                "timestamp": t["time"],
                "impact":    _impact_label(value),
            })
    return sorted(alerts, key=lambda x: x["value_usd"], reverse=True)


def _impact_label(value_usd: float) -> str:
    if value_usd >= 10_000_000:
        return "mega"
    if value_usd >= 1_000_000:
        return "large"
    if value_usd >= 500_000:
        return "medium"
    return "small"


def order_book_imbalance(bids: list[list], asks: list[list]) -> dict:
    """
    Calculate bid/ask imbalance from L2 order book.
    bids/asks: [[price, qty], ...]
    """
    bid_vol = sum(p * q for p, q in bids)
    ask_vol = sum(p * q for p, q in asks)
    total   = bid_vol + ask_vol

    if total == 0:
        return {"ratio": 0.5, "pressure": "neutral", "bid_vol": 0, "ask_vol": 0}

    ratio    = bid_vol / total
    pressure = "bullish" if ratio > 0.55 else "bearish" if ratio < 0.45 else "neutral"

    return {
        "ratio":    round(ratio, 4),
        "pressure": pressure,
        "bid_vol":  round(bid_vol, 2),
        "ask_vol":  round(ask_vol, 2),
               }
