"""
/backtest — strategy replay engine.
Powers: signals.js backtest results panel.
"""

from fastapi import APIRouter
from services.binance import get_klines
from indicators import rsi as calc_rsi, ema as calc_ema
from models.portfolio import BacktestRequest, BacktestResult
from utils.validators import validate_symbol, validate_timeframe

router = APIRouter()


@router.post("/run")
async def run_backtest(req: BacktestRequest) -> BacktestResult:
    sym     = validate_symbol(req.symbol)
    tf      = validate_timeframe(req.timeframe)
    candles = await get_klines(sym, interval=tf, limit=500)
    closes  = [c["close"] for c in candles]
    trades  = _run_strategy(req.strategy, closes)
    return _compute_result(req.strategy, sym, trades, closes)


def _run_strategy(strategy: str, closes: list[float]) -> list[dict]:
    trades = []
    if strategy == "momentum":
        for i in range(20, len(closes) - 1):
            r = calc_rsi(closes[:i + 1])
            if r < 35:
                entry = closes[i]
                exit_ = closes[i + 1]
                trades.append({"pnl_pct": (exit_ - entry) / entry * 100})
    elif strategy == "mean_reversion":
        for i in range(50, len(closes) - 1):
            e9  = calc_ema(closes[:i + 1], 9)
            e21 = calc_ema(closes[:i + 1], 21)
            if e9 > e21 and closes[i] > e9:
                entry = closes[i]
                exit_ = closes[i + 1]
                trades.append({"pnl_pct": (exit_ - entry) / entry * 100})
    elif strategy == "breakout":
        for i in range(20, len(closes) - 1):
            high20 = max(closes[i - 20:i])
            if closes[i] > high20 * 1.005:
                entry = closes[i]
                exit_ = closes[i + 1]
                trades.append({"pnl_pct": (exit_ - entry) / entry * 100})
    return trades


def _compute_result(
    strategy: str,
    symbol:   str,
    trades:   list[dict],
    closes:   list[float],
) -> BacktestResult:
    if not trades:
        return BacktestResult(
            strategy=strategy, symbol=symbol,
            win_rate=0, net_return_pct=0, profit_factor=0,
            total_trades=0, equity_curve=[closes[-1]],
        )
    wins    = [t for t in trades if t["pnl_pct"] > 0]
    losses  = [t for t in trades if t["pnl_pct"] <= 0]
    gross_p = sum(t["pnl_pct"] for t in wins)
    gross_l = abs(sum(t["pnl_pct"] for t in losses))
    net     = sum(t["pnl_pct"] for t in trades)

    equity = [100.0]
    for t in trades:
        equity.append(round(equity[-1] * (1 + t["pnl_pct"] / 100), 2))

    return BacktestResult(
        strategy=strategy,
        symbol=symbol,
        win_rate=round(len(wins) / len(trades) * 100, 2),
        net_return_pct=round(net, 2),
        profit_factor=round(gross_p / gross_l, 2) if gross_l > 0 else 99.0,
        total_trades=len(trades),
        equity_curve=equity[-50:],
  )
