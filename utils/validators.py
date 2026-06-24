"""
Validators — param guards used across routers.
"""

import re
from fastapi import HTTPException

VALID_TIMEFRAMES = {"1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"}
SYMBOL_RE = re.compile(r"^[A-Z]{2,10}(USDT|BTC|ETH|BNB|BUSD)$")


def validate_symbol(symbol: str) -> str:
    s = symbol.upper().strip()
    if not SYMBOL_RE.match(s):
        raise HTTPException(status_code=400, detail=f"Invalid symbol: {symbol}")
    return s


def validate_timeframe(tf: str) -> str:
    if tf not in VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{tf}'. Choose from: {sorted(VALID_TIMEFRAMES)}",
        )
    return tf


def validate_limit(limit: int, max_limit: int = 500) -> int:
    if limit < 1 or limit > max_limit:
        raise HTTPException(
            status_code=400, detail=f"limit must be between 1 and {max_limit}"
        )
    return limit
