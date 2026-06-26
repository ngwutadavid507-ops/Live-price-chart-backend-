"""
/ai — chat, analysis narratives, AI intel.
Powers: ai.js, aichat.js.
All AI calls go through Groq (llama-3.3-70b-versatile).
"""

from fastapi import APIRouter, HTTPException
from models.ai import ChatRequest, ChatResponse, AnalyseRequest, AnalyseResponse
from services.groq_client import chat, analyse_symbol
from cache.analysis_cache import analysis_cache
from cache.market_cache import market_cache
from utils.validators import validate_symbol

router = APIRouter()


@router.post("/chat", response_model=ChatResponse)
async def ai_chat(req: ChatRequest):
    messages       = [{"role": m.role, "content": m.content} for m in req.history]
    context_prefix = ""

    if req.symbol:
        sym    = validate_symbol(req.symbol)
        asset  = market_cache.get(sym)
        result = analysis_cache.get(sym)
        if asset and result:
            context_prefix = (
                f"[LIVE CONTEXT — {sym}] "
                f"Price: ${asset['price']}, "
                f"24h: {asset['change24h']:+.2f}%, "
                f"Trend: {result.trend.strength}, "
                f"RSI: {result.momentum.rsi}, "
                f"Signal: {result.signal.direction} "
                f"(confidence {result.signal.confidence}%)\n\n"
            )

    messages.append({
        "role":    "user",
        "content": context_prefix + req.message,
    })

    reply = await chat(messages)
    return ChatResponse(reply=reply, symbol=req.symbol)


@router.post("/analyse", response_model=AnalyseResponse)
async def ai_analyse(req: AnalyseRequest):
    sym    = validate_symbol(req.symbol)
    result = await analysis_cache.get_or_compute(sym)
    asset  = market_cache.get(sym)

    if not result:
        raise HTTPException(status_code=404, detail=f"No data for {sym}")

    context = (
        f"Symbol: {sym}\n"
        f"Price: ${asset['price'] if asset else 'N/A'}\n"
        f"Trend: {result.trend.strength} "
        f"(EMA9={result.trend.ema9}, EMA21={result.trend.ema21}, EMA50={result.trend.ema50})\n"
        f"RSI: {result.momentum.rsi} ({result.momentum.rsi_zone})\n"
        f"ATR: {result.volatility.atr} ({result.volatility.atr_pct:.2f}%)\n"
        f"Volume: {result.volume.volume_trend} (spike={result.volume.spike})\n"
        f"Signal: {result.signal.direction} | Confidence: {result.signal.confidence}%\n"
        f"Market Score: {result.market_score}\n"
    )
    if req.context:
        context += f"Additional context: {req.context}\n"

    narrative = await analyse_symbol(sym, context)

    return AnalyseResponse(
        symbol=sym,
        narrative=narrative,
        signal=result.signal.direction,
        confidence=result.signal.confidence,
    )


@router.get("/market-narrative")
async def market_narrative():
    hot  = market_cache.hot()[:5]
    summary_lines = [
        f"{h['symbol']}: ${h['price']} ({h['change24h']:+.2f}%)" for h in hot
    ]
    prompt = (
        "Based on this live market snapshot, give a 3-sentence professional "
        "market narrative covering dominant trend, key risk, and one opportunity:\n\n"
        + "\n".join(summary_lines)
    )
    reply = await chat([{"role": "user", "content": prompt}])
    return {"narrative": reply}
