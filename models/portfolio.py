from pydantic import BaseModel
from typing import Optional


class Position(BaseModel):
    symbol:        str
    amount:        float
    entry_price:   float
    current_price: float
    pnl:           float
    pnl_pct:       float
    side:          str


class PortfolioState(BaseModel):
    balance:     float
    pnl_24h:     float
    pct_24h:     float
    positions:   list[Position]
    total_value: float


class RiskMetrics(BaseModel):
    max_drawdown:  float
    sharpe_ratio:  float
    win_rate:      float
    profit_factor: float
    avg_rr:        float


class BacktestRequest(BaseModel):
    symbol:    str
    strategy:  str
    timeframe: str
    from_ts:   int
    to_ts:     int


class BacktestResult(BaseModel):
    strategy:       str
    symbol:         str
    win_rate:       float
    net_return_pct: float
    profit_factor:  float
    total_trades:   int
    equity_curve:   list[float]
