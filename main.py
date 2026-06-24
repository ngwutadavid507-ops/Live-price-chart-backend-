"""
Phoenix Terminal Backend — V8
Entry point. Mounts all routers, registers startup/shutdown hooks,
configures CORS and rate limiting.
"""

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from routers import markets, analysis, signals, orderflow, whales
from routers import portfolio, backtest, correlation, compare
from routers import watchlist, news, ai, journal, protools, settings
from cache.market_cache import market_cache
from cache.analysis_cache import analysis_cache
from websocket.manager import ws_manager

limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await market_cache.build()
    await analysis_cache.build()
    asyncio.create_task(market_cache.auto_refresh())
    asyncio.create_task(analysis_cache.auto_refresh())
    yield
    await ws_manager.shutdown()


app = FastAPI(
    title="Phoenix Terminal API",
    version="8.0.0",
    description="Real-time crypto intelligence backend for Phoenix AI Terminal",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(markets.router,     prefix="/markets",     tags=["Markets"])
app.include_router(analysis.router,    prefix="/analysis",    tags=["Analysis"])
app.include_router(signals.router,     prefix="/signals",     tags=["Signals"])
app.include_router(orderflow.router,   prefix="/orderflow",   tags=["Order Flow"])
app.include_router(whales.router,      prefix="/whales",      tags=["Whales"])
app.include_router(portfolio.router,   prefix="/portfolio",   tags=["Portfolio"])
app.include_router(backtest.router,    prefix="/backtest",    tags=["Backtest"])
app.include_router(correlation.router, prefix="/correlation", tags=["Correlation"])
app.include_router(compare.router,     prefix="/compare",     tags=["Compare"])
app.include_router(watchlist.router,   prefix="/watchlist",   tags=["Watchlist"])
app.include_router(news.router,        prefix="/news",        tags=["News"])
app.include_router(ai.router,          prefix="/ai",          tags=["AI"])
app.include_router(journal.router,     prefix="/journal",     tags=["Journal"])
app.include_router(protools.router,    prefix="/protools",    tags=["Pro Tools"])
app.include_router(settings.router,    prefix="/settings",    tags=["Settings"])


@app.get("/", tags=["System"])
async def root():
    mc = market_cache.meta()
    ac = analysis_cache.meta()
    return {
        "status": "online",
        "version": "8.0.0",
        "total_symbols":        mc["total_symbols"],
        "exchange_price_count": mc["exchange_count"],
        "aggregator_count":     mc["aggregator_count"],
        "source_log":           mc["source_log"],
        "cache_age_seconds":    mc["age_seconds"],
        "analysis_status":      ac["status"],
    }


@app.get("/health", tags=["System"])
async def health():
    return {
        "backend":           "healthy",
        "websocket_clients": ws_manager.client_count(),
        "cache_ready":       market_cache.is_ready(),
        "analysis_ready":    analysis_cache.is_ready(),
}
