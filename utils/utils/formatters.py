"""
Formatters — shared number/percentage formatting utilities.
"""

from datetime import datetime, timezone


def fmt_price(val: float) -> str:
    if val >= 1:
        return f"${val:,.2f}"
    return f"${val:.6f}"


def fmt_pct(val: float) -> str:
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.2f}%"


def fmt_compact(val: float) -> str:
    if abs(val) >= 1_000_000_000:
        return f"{val / 1_000_000_000:.1f}B"
    if abs(val) >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    if abs(val) >= 1_000:
        return f"{val / 1_000:.1f}K"
    return str(round(val, 2))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def timestamp_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)
