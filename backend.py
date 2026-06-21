from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import aiohttp
import asyncio
import json
import time
import random
import math
from collections import defaultdict
from typing import Optional, List, Dict, Any, Tuple

# ========== CONFIG ==========
COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_CHART = "https://api.coingecko.com/api/v3/coins/{id}/market_chart"
COINPAPRIKA_TICKERS = "https://api.coinpaprika.com/v1/tickers"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers?category=spot"
BINANCE_TICKERS = "https://api.binance.com/api/v3/ticker/24hr"
OKX_TICKERS = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
MEXC_TICKERS = "https://api.mexc.com/api/v3/ticker/24hr"
BINGX_TICKERS = "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr"

CACHE_TTL = 30
ANALYSIS_TTL = 30
WS_INTERVAL = 3
RATE_LIMIT_MAX = 50
RATE_LIMIT_WINDOW = 60
MAX_WS_CLIENTS = 100
MAX_CANDLES = 500  # Increased for better indicator accuracy

# ========== STATE ==========
cache = {"all": [], "hot": [], "ready": False, "last_update": 0, "id_map": {}, "source_log": []}
analysis_cache = {"data": {}, "ready": False, "last_update": 0}  # Separate analysis cache
rate_limits = defaultdict(list)
ws_clients = set()
http_session = None

# ========== HTTP CLIENT ==========
async def fetch_json(url, headers=None, params=None, timeout=15):
    global http_session
    if http_session is None:
        return None
    try:
        async with http_session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            text = await r.text()
            if r.status == 429:
                return {"_error": "rate_limited", "status": r.status}
            if r.status == 451:
                return {"_error": "blocked", "status": r.status}
            if r.status == 404:
                return {"_error": "not_found", "status": r.status}
            if r.status != 200:
                return {"_error": "http_error", "status": r.status, "body": text[:200]}
            return json.loads(text)
    except asyncio.TimeoutError:
        return {"_error": "timeout"}
    except Exception as e:
        return {"_error": "exception", "message": str(e)[:100]}

# ========== CANDLE FETCHING (ENHANCED) ==========
async def fetch_candles_for_analysis(symbol: str, days: str = "30") -> List[Dict]:
    """
    Fetch up to MAX_CANDLES for indicator calculation.
    Uses CoinGecko if available, falls back to demo.
    """
    symbol_upper = symbol.upper()
    coin_id = cache.get("id_map", {}).get(symbol_upper)
    
    if not coin_id:
        return generate_demo_candles(symbol_upper, count=200)
    
    url = COINGECKO_CHART.format(id=coin_id)
    params = {"vs_currency": "usd", "days": days}
    data = await fetch_json(url, params=params, timeout=20)
    
    if not data or not isinstance(data, dict) or data.get("_error"):
        return generate_demo_candles(symbol_upper, count=200)
    
    prices = data.get("prices", [])
    if not prices or len(prices) < 50:
        return generate_demo_candles(symbol_upper, count=200)
    
    # Convert price points to OHLC candles with better granularity
    total_points = len(prices)
    target_candles = min(MAX_CANDLES, max(200, total_points // 3))
    points_per_candle = max(1, total_points // target_candles)
    
    candles = []
    bucket = []
    bucket_start = None
    
    for ts, price in prices:
        if bucket_start is None:
            bucket_start = ts
        bucket.append((ts, price))
        
        if len(bucket) >= points_per_candle:
            opens = [p for _, p in bucket]
            highs = [p for _, p in bucket]
            lows = [p for _, p in bucket]
            candles.append({
                "time": bucket_start,
                "open": opens[0],
                "high": max(highs),
                "low": min(lows),
                "close": opens[-1]
            })
            bucket_start = None
            bucket = []
    
    if bucket:
        opens = [p for _, p in bucket]
        highs = [p for _, p in bucket]
        lows = [p for _, p in bucket]
        candles.append({
            "time": bucket_start,
            "open": opens[0],
            "high": max(highs),
            "low": min(lows),
            "close": opens[-1]
        })
    
    return candles[-MAX_CANDLES:]  # Return last MAX_CANDLES

def generate_demo_candles(symbol="BTC", count=200):
    seed = sum(ord(c) for c in symbol)
    rng = random.Random(seed)
    now = int(time.time() * 1000)
    base_price = 50000 + rng.random() * 50000
    
    if symbol == "BTC":
        base_price = 63900
    elif symbol == "ETH":
        base_price = 3450
    elif symbol == "SOL":
        base_price = 145
    
    candles = []
    for i in range(count):
        ts = now - (count - i) * 3600 * 1000
        change = (rng.random() - 0.48) * 0.02
        open_p = base_price
        close_p = base_price * (1 + change)
        high_p = max(open_p, close_p) * (1 + rng.random() * 0.005)
        low_p = min(open_p, close_p) * (1 - rng.random() * 0.005)
        candles.append({
            "time": ts,
            "open": round(open_p, 2),
            "high": round(high_p, 2),
            "low": round(low_p, 2),
            "close": round(close_p, 2)
        })
        base_price = close_p
    
    return candles

 # ========== TECHNICAL INDICATORS ==========

def calculate_ema(prices: List[float], period: int) -> List[float]:
    """Calculate Exponential Moving Average."""
    if len(prices) < period:
        return prices[:]
    
    multiplier = 2.0 / (period + 1)
    ema = [sum(prices[:period]) / period]
    
    for price in prices[period:]:
        ema.append((price - ema[-1]) * multiplier + ema[-1])
    
    # Pad beginning with SMA
    sma = ema[0]
    return [sma] * (period - 1) + ema

def calculate_rsi(prices: List[float], period: int = 14) -> List[float]:
    if len(prices) < period + 1:
        return [50.0] * len(prices)

    gains, losses = [], []

    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    rsi = [50.0] * period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            rsi.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi.append(100 - (100 / (1 + rs)))

    return rsi

def calculate_atr(candles: List[Dict], period: int = 14) -> List[float]:
    """Calculate Average True Range for volatility."""
    if len(candles) < 2:
        return [0.0]
    
    tr_values = []
    for i in range(1, len(candles)):
        high = candles[i]["high"]
        low = candles[i]["low"]
        prev_close = candles[i-1]["close"]
        
        tr1 = high - low
        tr2 = abs(high - prev_close)
        tr3 = abs(low - prev_close)
        
        tr_values.append(max(tr1, tr2, tr3))
    
    if len(tr_values) < period:
        return [sum(tr_values) / len(tr_values)] * len(tr_values) if tr_values else [0.0]
    
    atr = [sum(tr_values[:period]) / period]
    
    for i in range(period, len(tr_values)):
        atr.append((atr[-1] * (period - 1) + tr_values[i]) / period)
    
    # Pad beginning
    first_atr = atr[0]
    return [first_atr] * period + atr[1:]

def calculate_volume_profile(candles: List[Dict]) -> Dict:
    if len(candles) < 5:
        return {"avg_volume": 0, "volume_trend": "neutral", "volume_spike": False}

    volumes = []
    for c in candles:
        body = abs(c["close"] - c["open"])
        range_size = c["high"] - c["low"]
        volumes.append(body + (range_size * 0.5))

    avg_vol = sum(volumes) / len(volumes)
    recent = sum(volumes[-5:]) / 5

    return {
        "avg_volume": round(avg_vol, 6),
        "volume_trend": "increasing" if recent > avg_vol * 1.2 else "decreasing" if recent < avg_vol * 0.8 else "neutral",
        "volume_spike": recent > avg_vol * 2
    }

def find_support_resistance(candles: List[Dict], lookback: int = 20) -> Tuple[List[float], List[float]]:
    """Find support and resistance levels from recent pivots."""
    if len(candles) < lookback * 2:
        return [], []
    
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    
    # Simple pivot detection
    resistance_levels = []
    support_levels = []
    
    for i in range(lookback, len(candles) - lookback):
        # Local high = resistance
        if all(highs[i] >= highs[j] for j in range(i - lookback, i + lookback + 1) if j != i):
            resistance_levels.append(highs[i])
        # Local low = support
        if all(lows[i] <= lows[j] for j in range(i - lookback, i + lookback + 1) if j != i):
            support_levels.append(lows[i])
    
    # Cluster nearby levels (within 2%)
    def cluster_levels(levels, threshold=0.02):
        if not levels:
            return []
        levels.sort()
        clusters = [[levels[0]]]
        for level in levels[1:]:
            if abs(level - clusters[-1][-1]) / clusters[-1][-1] < threshold:
                clusters[-1].append(level)
            else:
                clusters.append([level])
        return [sum(c) / len(c) for c in clusters]
    
    return cluster_levels(support_levels[-5:]), cluster_levels(resistance_levels[-5:])

def calculate_trend(candles: List[Dict]) -> Dict:
    """Calculate trend using EMA crossover."""
    closes = [c["close"] for c in candles]
    
    if len(closes) < 21:
        return {"direction": "neutral", "strength": 0, "ema9": 0, "ema21": 0}
    
    ema9 = calculate_ema(closes, 9)
    ema21 = calculate_ema(closes, 21)
    
    current_ema9 = ema9[-1]
    current_ema21 = ema21[-1]
    prev_ema9 = ema9[-2] if len(ema9) > 1 else current_ema9
    prev_ema21 = ema21[-2] if len(ema21) > 1 else current_ema21
    
    # Trend direction
    if current_ema9 > current_ema21 and prev_ema9 <= prev_ema21:
        direction = "bullish"
    elif current_ema9 < current_ema21 and prev_ema9 >= prev_ema21:
        direction = "bearish"
    elif current_ema9 > current_ema21:
        direction = "bullish"
    elif current_ema9 < current_ema21:
        direction = "bearish"
    else:
        direction = "neutral"
    
    # Trend strength (0-100)
    diff_pct = abs(current_ema9 - current_ema21) / current_ema21 * 100
    strength = min(100, diff_pct * 10)
    
    return {
        "direction": direction,
        "strength": round(strength, 2),
        "ema9": round(current_ema9, 2),
        "ema21": round(current_ema21, 2)
    }

def calculate_momentum(candles: List[Dict]) -> Dict:
    """Calculate momentum using RSI + price acceleration."""
    closes = [c["close"] for c in candles]
    
    if len(closes) < 15:
        return {"rsi": 50, "signal": "neutral", "acceleration": 0}
    
    rsi_values = calculate_rsi(closes, 14)
    current_rsi = rsi_values[-1]
    
    # Price acceleration (rate of change of rate of change)
    if len(closes) >= 3:
        roc1 = (closes[-1] - closes[-2]) / closes[-2] * 100
        roc2 = (closes[-2] - closes[-3]) / closes[-3] * 100
        acceleration = roc1 - roc2
    else:
        acceleration = 0
    
    # Signal based on RSI + acceleration
    if current_rsi > 70 and acceleration > 0:
        signal = "strong_buy" if current_rsi < 80 else "overbought"
    elif current_rsi > 55:
        signal = "buy"
    elif current_rsi < 30 and acceleration < 0:
        signal = "strong_sell" if current_rsi > 20 else "oversold"
    elif current_rsi < 45:
        signal = "sell"
    else:
        signal = "neutral"
    
    return {
        "rsi": round(current_rsi, 2),
        "signal": signal,
        "acceleration": round(acceleration, 4)
    }

def calculate_market_score(trend: Dict, momentum: Dict, volatility: float, volume: Dict) -> Dict:
    """Calculate overall market strength score (0-100)."""
    score = 50  # Neutral base
    
    # Trend contribution (0-30)
    if trend["direction"] == "bullish":
        score += min(30, trend["strength"] * 0.3)
    elif trend["direction"] == "bearish":
        score -= min(30, trend["strength"] * 0.3)
    
    # Momentum contribution (0-25)
    rsi = momentum["rsi"]
    if rsi > 50:
        score += min(25, (rsi - 50) * 0.5)
    else:
        score -= min(25, (50 - rsi) * 0.5)
    
    # Volume contribution (0-20)
    if volume["volume_trend"] == "increasing":
        score += 10
    if volume["volume_spike"]:
        score += 10
    
    # Volatility penalty (0-25)
    if volatility > 5:  # High volatility
        score -= 10
    elif volatility < 1:  # Too low, no movement
        score -= 5
    
    # Clamp to 0-100
    score = max(0, min(100, score))
    
    # Overall state
    if score >= 70:
        state = "strong"
    elif score >= 55:
        state = "bullish"
    elif score >= 45:
        state = "neutral"
    elif score >= 30:
        state = "bearish"
    else:
        state = "weak"
    
    return {
        "score": round(score, 2),
        "state": state
                       }

# ========== ANALYSIS ENGINE ==========
async def analyze_symbol(symbol: str) -> Dict:
    """Complete technical analysis for a symbol."""
    candles = await fetch_candles_for_analysis(symbol, days="30")
    
    if len(candles) < 50:
        return {
            "symbol": symbol,
            "trend": {"direction": "neutral", "strength": 0},
            "momentum": {"rsi": 50, "signal": "neutral", "acceleration": 0},
            "volatility": {"atr": 0, "atr_percent": 0},
            "volume": {"avg_volume": 0, "volume_trend": "neutral", "volume_spike": False},
            "levels": {"support": [], "resistance": []},
            "market_score": {"score": 50, "state": "neutral"},
            "signal": {"recommendation": "hold", "confidence": 0},
            "timestamp": int(time.time() * 1000)
        }
    
    # Calculate all metrics
    trend = calculate_trend(candles)
    momentum = calculate_momentum(candles)
    
    atr_values = calculate_atr(candles, 14)
    current_atr = atr_values[-1] if atr_values else 0
    current_price = candles[-1]["close"]
    atr_percent = (current_atr / current_price) * 100 if current_price > 0 else 0
    
    volatility = {
        "atr": round(current_atr, 4),
        "atr_percent": round(atr_percent, 4)
    }
    
    volume = calculate_volume_profile(candles)
    support, resistance = find_support_resistance(candles)
    
    market_score = calculate_market_score(trend, momentum, atr_percent, volume)
    
    # Generate trading signal
    confidence = 0
    recommendation = "hold"
    
    if trend["direction"] == "bullish" and momentum["signal"] in ["buy", "strong_buy"]:
        confidence = min(100, (trend["strength"] + (momentum["rsi"] - 50)) / 2)
        recommendation = "buy" if confidence < 70 else "strong_buy"
    elif trend["direction"] == "bearish" and momentum["signal"] in ["sell", "strong_sell"]:
        confidence = min(100, (trend["strength"] + (50 - momentum["rsi"])) / 2)
        recommendation = "sell" if confidence < 70 else "strong_sell"
    
    return {
        "symbol": symbol,
        "trend": trend,
        "momentum": momentum,
        "volatility": volatility,
        "volume": volume,
        "levels": {
            "support": [round(s, 2) for s in support[-3:]],
            "resistance": [round(r, 2) for r in resistance[-3:]]
        },
        "market_score": market_score,
        "signal": {
            "recommendation": recommendation,
            "confidence": round(confidence, 2)
        },
        "timestamp": int(time.time() * 1000)
    }

async def build_analysis_cache():
    """Background task: Analyze top 100 hot symbols every 30s."""
    global analysis_cache
    
    if not cache["ready"] or not cache["hot"]:
        return
    
    print("[ANALYSIS] Building analysis cache...")
    results = {}
    
    # Analyze top 50 symbols (limit to avoid rate limits)
    symbols_to_analyze = cache["hot"][:50]
    
    for token in symbols_to_analyze:
        sym = token["symbol"]
        try:
            results[sym] = await analyze_symbol(sym)
        except Exception as e:
            print(f"[ANALYSIS ERROR] {sym}: {e}")
    
    analysis_cache["data"] = results
    analysis_cache["ready"] = True
    analysis_cache["last_update"] = time.time()
    print(f"[ANALYSIS] Cached {len(results)} symbols")

async def analysis_background_task():
    """Run analysis every ANALYSIS_TTL seconds."""
    while True:
        try:
            await build_analysis_cache()
        except Exception as e:
            print(f"[ANALYSIS BG ERROR] {type(e).__name__}: {e}")
        await asyncio.sleep(ANALYSIS_TTL)

# ========== EXCHANGE FETCHERS (same as before, abbreviated) ==========
async def fetch_bybit():
    data = await fetch_json(BYBIT_TICKERS, timeout=15)
    if isinstance(data, dict) and data.get("_error"):
        print(f"[BYBIT] ERROR: {data.get('_error')} status={data.get('status')}")
        return [], {}
    if not isinstance(data, dict):
        return [], {}
    tickers = data.get("result", {}).get("list", [])
    result = []
    result_map = {}
    for t in tickers:
        if not isinstance(t, dict):
            continue
        raw = t.get("symbol", "")
        if not raw.endswith("USDT"):
            continue
        sym = raw[:-4].upper()
        price = float(t.get("lastPrice") or 0)
        if price <= 0:
            continue
        high = float(t.get("highPrice24h") or 0)
        low = float(t.get("lowPrice24h") or 0)
        coin = {
            "symbol": sym, "name": sym, "price": price,
            "change": float(t.get("price24hPcnt") or 0) * 100,
            "volume": float(t.get("turnover24h") or 0),
            "market_cap": 0,
            "high_24h": high if high > 0 else price * 1.02,
            "low_24h": low if low > 0 else price * 0.98,
            "source": "bybit", "price_confidence": "exchange"
        }
        result.append(coin)
        result_map[sym] = coin
    print(f"[BYBIT] {len(result)} tickers")
    return result, result_map

async def fetch_binance():
    data = await fetch_json(BINANCE_TICKERS, timeout=15)
    if isinstance(data, dict) and data.get("_error"):
        print(f"[BINANCE] ERROR: {data.get('_error')} status={data.get('status')}")
        return [], {}
    if not isinstance(data, list):
        return [], {}
    result = []
    result_map = {}
    for t in data:
        if not isinstance(t, dict):
            continue
        raw = t.get("symbol", "")
        if not raw.endswith("USDT"):
            continue
        sym = raw[:-4].upper()
        price = float(t.get("lastPrice") or 0)
        if price <= 0:
            continue
        high = float(t.get("highPrice") or 0)
        low = float(t.get("lowPrice") or 0)
        coin = {
            "symbol": sym, "name": sym, "price": price,
            "change": float(t.get("priceChangePercent") or 0),
            "volume": float(t.get("quoteVolume") or 0),
            "market_cap": 0,
            "high_24h": high if high > 0 else price * 1.02,
            "low_24h": low if low > 0 else price * 0.98,
            "source": "binance", "price_confidence": "exchange"
        }
        result.append(coin)
        result_map[sym] = coin
    print(f"[BINANCE] {len(result)} tickers")
    return result, result_map

async def fetch_okx():
    data = await fetch_json(OKX_TICKERS, timeout=15)
    if isinstance(data, dict) and data.get("_error"):
        print(f"[OKX] ERROR: {data.get('_error')} status={data.get('status')}")
        return [], {}
    if not isinstance(data, dict):
        return [], {}
    tickers = data.get("data", [])
    result = []
    result_map = {}
    for t in tickers:
        if not isinstance(t, dict):
            continue
        raw = t.get("instId", "")
        if not raw.endswith("-USDT"):
            continue
        sym = raw[:-5].upper()
        price = float(t.get("last") or 0)
        if price <= 0:
            continue
        high = float(t.get("high24h") or 0)
        low = float(t.get("low24h") or 0)
        coin = {
            "symbol": sym, "name": sym, "price": price,
            "change": float(t.get("change24h") or 0) * 100,
            "volume": float(t.get("volCcy24h") or 0) * price,
            "market_cap": 0,
            "high_24h": high if high > 0 else price * 1.02,
            "low_24h": low if low > 0 else price * 0.98,
            "source": "okx", "price_confidence": "exchange"
        }
        result.append(coin)
        result_map[sym] = coin
    print(f"[OKX] {len(result)} tickers")
    return result, result_map

async def fetch_mexc():
    data = await fetch_json(MEXC_TICKERS, timeout=15)
    if isinstance(data, dict) and data.get("_error"):
        print(f"[MEXC] ERROR: {data.get('_error')} status={data.get('status')}")
        return [], {}
    if not isinstance(data, list):
        return [], {}
    result = []
    result_map = {}
    for t in data:
        if not isinstance(t, dict):
            continue
        raw = t.get("symbol", "")
        if not raw.endswith("USDT"):
            continue
        sym = raw[:-4].upper()
        price = float(t.get("lastPrice") or 0)
        if price <= 0:
            continue
        high = float(t.get("highPrice") or 0)
        low = float(t.get("lowPrice") or 0)
        coin = {
            "symbol": sym, "name": sym, "price": price,
            "change": float(t.get("priceChangePercent") or 0),
            "volume": float(t.get("quoteVolume") or 0),
            "market_cap": 0,
            "high_24h": high if high > 0 else price * 1.02,
            "low_24h": low if low > 0 else price * 0.98,
            "source": "mexc", "price_confidence": "exchange"
        }
        result.append(coin)
        result_map[sym] = coin
    print(f"[MEXC] {len(result)} tickers")
    return result, result_map

async def fetch_bingx():
    data = await fetch_json(BINGX_TICKERS, timeout=15)
    if isinstance(data, dict) and data.get("_error"):
        print(f"[BINGX] ERROR: {data.get('_error')} status={data.get('status')}")
        return [], {}
    if not isinstance(data, dict):
        return [], {}
    tickers = data.get("data", [])
    if not isinstance(tickers, list):
        tickers = data if isinstance(data, list) else []
    result = []
    result_map = {}
    for t in tickers:
        if not isinstance(t, dict):
            continue
        raw = t.get("symbol", "")
        if not raw or "USDT" not in raw:
            continue
        sym = raw.replace("-USDT", "").replace("USDT", "").upper()
        if not sym or len(sym) > 10:
            continue
        price = float(t.get("lastPrice") or t.get("last") or 0)
        if price <= 0:
            continue
        high = float(t.get("highPrice") or t.get("high24h") or 0)
        low = float(t.get("lowPrice") or t.get("low24h") or 0)
        change_raw = t.get("priceChangePercent") or t.get("priceChange") or 0
        if isinstance(change_raw, str):
            change_raw = change_raw.replace("%", "").strip()
        change = float(change_raw)
        volume = float(t.get("quoteVolume") or t.get("volume") or 0)
        coin = {
            "symbol": sym, "name": sym, "price": price,
            "change": change,
            "volume": volume,
            "market_cap": 0,
            "high_24h": high if high > 0 else price * 1.02,
            "low_24h": low if low > 0 else price * 0.98,
            "source": "bingx", "price_confidence": "exchange"
        }
        result.append(coin)
        result_map[sym] = coin
    print(f"[BINGX] {len(result)} tickers")
    return result, result_map

async def fetch_coingecko():
    params = {
        "vs_currency": "usd", "order": "market_cap_desc",
        "per_page": 250, "page": 1, "sparkline": "false",
        "price_change_percentage": "24h"
    }
    data = await fetch_json(COINGECKO_MARKETS, params=params, timeout=20)
    if isinstance(data, dict) and data.get("_error"):
        print(f"[COINGECKO] ERROR: {data.get('_error')} status={data.get('status')}")
        return [], {}
    if not isinstance(data, list):
        return [], {}
    result = []
    id_map = {}
    for coin in data:
        if not isinstance(coin, dict):
            continue
        sym = coin.get("symbol", "").upper()
        result.append({
            "symbol": sym, "name": coin.get("name"),
            "price": float(coin.get("current_price") or 0),
            "change": float(coin.get("price_change_percentage_24h") or 0),
            "volume": float(coin.get("total_volume") or 0),
            "market_cap": float(coin.get("market_cap") or 0),
            "high_24h": float(coin.get("high_24h") or 0),
            "low_24h": float(coin.get("low_24h") or 0),
            "source": "coingecko", "price_confidence": "aggregator"
        })
        if coin.get("id"):
            id_map[sym] = coin["id"]
    print(f"[COINGECKO] {len(result)} coins")
    return result, id_map

async def fetch_coinpaprika():
    data = await fetch_json(COINPAPRIKA_TICKERS, timeout=20)
    if isinstance(data, dict) and data.get("_error"):
        print(f"[COINPAPRIKA] ERROR: {data.get('_error')} status={data.get('status')}")
        return []
    if not isinstance(data, list):
        return []
    result = []
    for coin in data[:250]:
        if not isinstance(coin, dict):
            continue
        usd = coin.get("quotes", {}).get("USD", {})
        if not usd:
            continue
        sym = coin.get("symbol", "").upper()
        price = float(usd.get("price") or 0)
        change = float(usd.get("percent_change_24h") or 0)
        result.append({
            "symbol": sym, "name": coin.get("name"),
            "price": price, "change": change,
            "volume": float(usd.get("volume_24h") or 0),
            "market_cap": float(usd.get("market_cap") or 0),
            "high_24h": price * (1 + abs(change) / 100 * 0.6) if price > 0 else 0,
            "low_24h": price * (1 - abs(change) / 100 * 0.6) if price > 0 else 0,
            "source": "coinpaprika", "price_confidence": "backup"
        })
    print(f"[COINPAPRIKA] {len(result)} coins")
    return result

async def merge_all(exchanges, cg_data, cg_id_map, cp_data):
    merged = {}
    id_map = {}
    source_log = []
    exchange_sources = ["bybit", "binance", "okx", "mexc", "bingx"]
    for i, (ex_data, ex_map) in enumerate(exchanges):
        src = exchange_sources[i] if i < len(exchange_sources) else f"ex{i}"
        added = 0
        for coin in ex_data:
            sym = coin["symbol"]
            if sym not in merged:
                merged[sym] = coin.copy()
                added += 1
        if ex_data:
            source_log.append(f"{src}:{len(ex_data)}(+{added})")
    cg_added = 0
    for coin in cg_data:
        sym = coin["symbol"]
        if cg_id_map.get(sym):
            id_map[sym] = cg_id_map[sym]
        if sym not in merged:
            merged[sym] = coin.copy()
            cg_added += 1
        else:
            if coin.get("name") and (not merged[sym].get("name") or merged[sym].get("name") == sym):
                merged[sym]["name"] = coin["name"]
            if coin.get("market_cap", 0) > 0:
                merged[sym]["market_cap"] = coin["market_cap"]
            if merged[sym].get("high_24h", 0) == 0 and coin.get("high_24h", 0) > 0:
                merged[sym]["high_24h"] = coin["high_24h"]
            if merged[sym].get("low_24h", 0) == 0 and coin.get("low_24h", 0) > 0:
                merged[sym]["low_24h"] = coin["low_24h"]
    if cg_data:
        source_log.append(f"cg:{len(cg_data)}(+{cg_added})")
    cp_added = 0
    for coin in cp_data:
        sym = coin["symbol"]
        if sym not in merged:
            merged[sym] = coin.copy()
            cp_added += 1
        else:
            if coin.get("market_cap", 0) > 0 and merged[sym].get("market_cap", 0) == 0:
                merged[sym]["market_cap"] = coin["market_cap"]
    if cp_data:
        source_log.append(f"cp:{len(cp_data)}(+{cp_added})")
    fixed = 0
    for coin in merged.values():
        price = coin.get("price", 0)
        change = coin.get("change", 0)
        if price > 0:
            if coin.get("high_24h", 0) == 0:
                coin["high_24h"] = price * (1 + abs(change) / 100 * 0.5)
                fixed += 1
            if coin.get("low_24h", 0) == 0:
                coin["low_24h"] = price * (1 - abs(change) / 100 * 0.5)
                fixed += 1
            if coin["high_24h"] <= coin["low_24h"]:
                coin["high_24h"] = price * 1.01
                coin["low_24h"] = price * 0.99
    if fixed > 0:
        source_log.append(f"fixed:{fixed}")
    result = list(merged.values())
    result.sort(key=lambda x: (x.get("price_confidence") == "exchange", x.get("volume", 0)), reverse=True)
    return result, id_map, source_log

async def build_cache():
    global cache
    print("[BUILD] Fetching from 7 sources...")
    tasks = [
        asyncio.create_task(fetch_bybit()),
        asyncio.create_task(fetch_binance()),
        asyncio.create_task(fetch_okx()),
        asyncio.create_task(fetch_mexc()),
        asyncio.create_task(fetch_bingx()),
        asyncio.create_task(fetch_coingecko()),
        asyncio.create_task(fetch_coinpaprika()),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    exchanges = []
    for i, r in enumerate(results[:5]):
        names = ["bybit", "binance", "okx", "mexc", "bingx"]
        if isinstance(r, Exception):
            print(f"[{names[i].upper()}] EXCEPTION: {r}")
            exchanges.append(([], {}))
        elif isinstance(r, tuple) and len(r) == 2:
            exchanges.append(r)
        else:
            exchanges.append(([], {}))
    cg_result = results[5]
    if isinstance(cg_result, Exception):
        print(f"[COINGECKO] EXCEPTION: {cg_result}")
        cg_data, cg_id_map = [], {}
    elif isinstance(cg_result, tuple) and len(cg_result) == 2:
        cg_data, cg_id_map = cg_result
    else:
        cg_data, cg_id_map = [], {}
    cp_result = results[6]
    if isinstance(cp_result, Exception):
        print(f"[COINPAPRIKA] EXCEPTION: {cp_result}")
        cp_data = []
    elif isinstance(cp_result, list):
        cp_data = cp_result
    else:
        cp_data = []
    if not any(ex[0] for ex in exchanges) and not cg_data and not cp_data:
        print("[CACHE] ALL SOURCES FAILED")
        if not cache["all"]:
            cache["ready"] = True
        return
    merged, id_map, source_log = await merge_all(exchanges, cg_data, cg_id_map, cp_data)
    cache["all"] = merged
    cache["hot"] = merged[:100]
    cache["id_map"] = id_map
    cache["ready"] = True
    cache["last_update"] = time.time()
    cache["source_log"] = source_log
    ex_count = sum(1 for c in merged if c.get("price_confidence") == "exchange")
    print(f"[CACHE] {len(merged)} symbols | {ex_count} exchange | {len(merged)-ex_count} other | {source_log}")

async def background_fetcher():
    while True:
        try:
            await build_cache()
        except Exception as e:
            print(f"[BG ERROR] {type(e).__name__}: {e}")
        await asyncio.sleep(CACHE_TTL)

async def ws_broadcaster():
    while True:
        await asyncio.sleep(WS_INTERVAL)
        if not ws_clients or not cache["ready"]:
            continue
        payload = {
            "type": "hot",
            "data": cache["hot"],
            "timestamp": int(time.time() * 1000)
        }
        dead = set()
        for ws in ws_clients:
            try:
                await ws.send_json(payload)
            except:
                dead.add(ws)
        for ws in dead:
            ws_clients.discard(ws)

def check_rate_limit(ip):
    now = time.time()
    rate_limits[ip] = [t for t in rate_limits[ip] if now - t < RATE_LIMIT_WINDOW]
    if len(rate_limits[ip]) >= RATE_LIMIT_MAX:
        return False
    rate_limits[ip].append(now)
    return True

@asynccontextmanager
async def lifespan(app):
    global http_session
    http_session = aiohttp.ClientSession()
    print("[STARTUP] Phoenix v8 - Phase 2: Market Analysis Engine")
    tasks = [
        asyncio.create_task(background_fetcher()),
        asyncio.create_task(ws_broadcaster()),
        asyncio.create_task(analysis_background_task())
    ]
    yield
    for t in tasks:
        t.cancel()
    await http_session.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def home():
    try:
        ex_count = sum(1 for c in cache.get("all", []) if c.get("price_confidence") == "exchange")
        return {
            "status": "running", "version": "v8",
            "symbols": len(cache.get("all", [])),
            "exchange_prices": ex_count,
            "aggregator_prices": len(cache.get("all", [])) - ex_count,
            "sources": ["bybit", "binance", "okx", "mexc", "bingx", "coingecko", "coinpaprika"],
            "source_log": cache.get("source_log", []),
            "cache_age": int(time.time() - cache.get("last_update", 0)),
            "analysis_ready": analysis_cache.get("ready", False),
            "analysis_symbols": len(analysis_cache.get("data", {}))
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/health")
async def health():
    try:
        ex_count = sum(1 for c in cache.get("all", []) if c.get("price_confidence") == "exchange")
        return {
            "status": "ok", "version": "v8",
            "cache_ready": cache.get("ready", False),
            "symbols": len(cache.get("all", [])),
            "exchange_prices": ex_count,
            "ws_clients": len(ws_clients),
            "source_log": cache.get("source_log", []),
            "cache_age_seconds": int(time.time() - cache.get("last_update", 0)),
            "analysis_ready": analysis_cache.get("ready", False),
            "analysis_count": len(analysis_cache.get("data", {}))
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/symbols")
async def symbols(request: Request):
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")
    if not cache["ready"]:
        await build_cache()
    return cache["all"]

@app.get("/candles/{symbol}")
async def candles(symbol: str, request: Request, days: str = "7"):
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")
    symbol_upper = symbol.upper()
    coin_id = cache.get("id_map", {}).get(symbol_upper)
    if not coin_id:
        return generate_demo_candles(symbol_upper)
    url = COINGECKO_CHART.format(id=coin_id)
    params = {"vs_currency": "usd", "days": days}
    data = await fetch_json(url, params=params, timeout=20)
    if not data or not isinstance(data, dict) or data.get("_error"):
        return generate_demo_candles(symbol_upper)
    prices = data.get("prices", [])
    if not prices or len(prices) < 10:
        return generate_demo_candles(symbol_upper)
    total_points = len(prices)
    candles_per_agg = max(1, total_points // 100)
    candles = []
    bucket = []
    bucket_start = None
    for ts, price in prices:
        if bucket_start is None:
            bucket_start = ts
        bucket.append((ts, price))
        if len(bucket) >= candles_per_agg:
            opens = [p for _, p in bucket]
            candles.append({
                "time": bucket_start,
                "open": opens[0],
                "high": max(p for _, p in bucket),
                "low": min(p for _, p in bucket),
                "close": opens[-1]
            })
            bucket_start = None
            bucket = []
    if bucket:
        opens = [p for _, p in bucket]
        candles.append({
            "time": bucket_start,
            "open": opens[0],
            "high": max(p for _, p in bucket),
            "low": min(p for _, p in bucket),
            "close": opens[-1]
        })
    return candles

# ========== NEW PHASE 2 ENDPOINTS ==========

@app.get("/analyze/{symbol}")
async def analyze_endpoint(symbol: str, request: Request):
    """Get pre-computed analysis for a symbol."""
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")
    
    symbol_upper = symbol.upper()
    
    # Return cached analysis if available
    if analysis_cache["ready"] and symbol_upper in analysis_cache["data"]:
        return analysis_cache["data"][symbol_upper]
    
    # Compute on-demand if not in cache
    return await analyze_symbol(symbol_upper)

@app.get("/trending")
async def trending(request: Request):
    """Get top trending coins by market score."""
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")
    
    # If cache not ready, try to build it quickly or return what we have
    if not analysis_cache["ready"] or not analysis_cache["data"]:
        # Try to analyze top 10 on-demand for quick response
        quick_results = []
        if cache["ready"] and cache["hot"]:
            for token in cache["hot"][:10]:
                try:
                    quick_results.append(await analyze_symbol(token["symbol"]))
                except Exception as e:
                    print(f"[TRENDING QUICK] {token['symbol']}: {e}")
        
        if quick_results:
            sorted_results = sorted(
                quick_results,
                key=lambda x: x.get("market_score", {}).get("score", 0),
                reverse=True
            )
            return {
                "status": "partial",
                "message": "Full analysis cache still building",
                "count": len(sorted_results),
                "data": sorted_results
            }
        
        return {"status": "not_ready", "message": "Analysis cache building, try again in 30s", "data": []}
    
    # Normal cached response
    sorted_analysis = sorted(
        analysis_cache["data"].values(),
        key=lambda x: x.get("market_score", {}).get("score", 0),
        reverse=True
    )
    
    return {
        "status": "ok",
        "count": len(sorted_analysis),
        "data": sorted_analysis[:20]
    }

@app.get("/market-summary")
async def market_summary(request: Request):
    """Overall market health summary."""
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")
    
    # If cache not ready, compute quick summary from hot symbols
    if not analysis_cache["ready"] or not analysis_cache["data"]:
        quick_data = []
        if cache["ready"] and cache["hot"]:
            for token in cache["hot"][:20]:
                try:
                    quick_data.append(await analyze_symbol(token["symbol"]))
                except:
                    pass
        
        if quick_data:
            bullish = sum(1 for d in quick_data if d.get("trend", {}).get("direction") == "bullish")
            bearish = sum(1 for d in quick_data if d.get("trend", {}).get("direction") == "bearish")
            neutral = sum(1 for d in quick_data if d.get("trend", {}).get("direction") == "neutral")
            avg_score = sum(d.get("market_score", {}).get("score", 50) for d in quick_data) / len(quick_data)
            
            return {
                "status": "partial",
                "message": "Full analysis cache still building",
                "total_analyzed": len(quick_data),
                "trend_distribution": {"bullish": bullish, "bearish": bearish, "neutral": neutral},
                "average_market_score": round(avg_score, 2),
                "top_bullish": sorted(
                    [d for d in quick_data if d.get("trend", {}).get("direction") == "bullish"],
                    key=lambda x: x.get("market_score", {}).get("score", 0),
                    reverse=True
                )[:3],
                "top_bearish": sorted(
                    [d for d in quick_data if d.get("trend", {}).get("direction") == "bearish"],
                    key=lambda x: x.get("market_score", {}).get("score", 0),
                    reverse=True
                )[:3]
            }
        
        return {"status": "not_ready", "message": "Analysis cache building, try again in 30s"}
    
    # Normal cached response
    data = analysis_cache["data"].values()
    
    bullish = sum(1 for d in data if d.get("trend", {}).get("direction") == "bullish")
    bearish = sum(1 for d in data if d.get("trend", {}).get("direction") == "bearish")
    neutral = sum(1 for d in data if d.get("trend", {}).get("direction") == "neutral")
    
    avg_score = sum(d.get("market_score", {}).get("score", 50) for d in data) / len(data) if data else 50
    
    return {
        "status": "ok",
        "total_analyzed": len(data),
        "trend_distribution": {"bullish": bullish, "bearish": bearish, "neutral": neutral},
        "average_market_score": round(avg_score, 2),
        "top_bullish": sorted(
            [d for d in data if d.get("trend", {}).get("direction") == "bullish"],
            key=lambda x: x.get("market_score", {}).get("score", 0),
            reverse=True
        )[:5],
        "top_bearish": sorted(
            [d for d in data if d.get("trend", {}).get("direction") == "bearish"],
            key=lambda x: x.get("market_score", {}).get("score", 0),
            reverse=True
        )[:5]
    }


@app.get("/market-summary")
async def market_summary(request: Request):
    """Overall market health summary."""
    if not check_rate_limit(request.client.host):
        raise HTTPException(429, "Rate limit")
    
    if not analysis_cache["ready"]:
        return {"status": "not_ready"}
    
    data = analysis_cache["data"].values()
    
    bullish = sum(1 for d in data if d.get("trend", {}).get("direction") == "bullish")
    bearish = sum(1 for d in data if d.get("trend", {}).get("direction") == "bearish")
    neutral = sum(1 for d in data if d.get("trend", {}).get("direction") == "neutral")
    
    avg_score = sum(d.get("market_score", {}).get("score", 50) for d in data) / len(data) if data else 50
    
    return {
        "status": "ok",
        "total_analyzed": len(data),
        "trend_distribution": {"bullish": bullish, "bearish": bearish, "neutral": neutral},
        "average_market_score": round(avg_score, 2),
        "top_bullish": sorted(
            [d for d in data if d.get("trend", {}).get("direction") == "bullish"],
            key=lambda x: x.get("market_score", {}).get("score", 0),
            reverse=True
        )[:5],
        "top_bearish": sorted(
            [d for d in data if d.get("trend", {}).get("direction") == "bearish"],
            key=lambda x: x.get("market_score", {}).get("score", 0),
            reverse=True
        )[:5]
    }

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    if len(ws_clients) >= MAX_WS_CLIENTS:
        await ws.close(code=1008)
        return
    await ws.accept()
    ws_clients.add(ws)
    if cache["ready"]:
        await ws.send_json({
            "type": "hot",
            "data": cache["hot"],
            "timestamp": int(time.time() * 1000)
        })
    try:
        while True:
            msg = await asyncio.wait_for(ws.receive_text(), timeout=30)
            if msg == "ping":
                await ws.send_json({"type": "pong"})
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    finally:
        ws_clients.discard(ws)
