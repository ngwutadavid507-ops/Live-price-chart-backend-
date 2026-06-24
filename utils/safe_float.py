"""
safe_float — handles Bybit's empty string numeric fields.
Always import this instead of float() when parsing exchange data.
"""


def safe_float(value, default: float = 0.0) -> float:
    """Convert any value to float safely. Returns default on failure."""
    if value is None or value == "" or value == "None":
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default
