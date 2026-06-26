"""
Groq Client — powers all /ai endpoints.
Model: llama-3.3-70b-versatile (same as Phoenix Docs).
"""

import httpx
from config import cfg

BASE    = "https://api.groq.com/openai/v1/chat/completions"
TIMEOUT = 30

SYSTEM_PROMPT = """You are Phoenix AI — a professional crypto market intelligence assistant
built into the Phoenix Terminal. You have access to real-time market data, technical
analysis, order flow insights, and whale activity tracking.

Your responses are:
- Concise and data-driven
- Direct — no fluff, no disclaimers about not being financial advice unless asked
- In the language of professional traders (use proper terms: confluence, invalidation,
  liquidity sweep, OB, FVG, CVD, etc.)
- Structured when needed (bullet points for multi-part answers)

When a symbol is provided as context, weave it into your answer naturally."""


async def chat(messages: list[dict], system: str = SYSTEM_PROMPT) -> str:
    headers = {
        "Authorization": f"Bearer {cfg.GROQ_API_KEY}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":       cfg.GROQ_MODEL,
        "max_tokens":  1024,
        "messages":    [{"role": "system", "content": system}] + messages,
        "temperature": 0.7,
    }
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        r = await client.post(BASE, json=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
    return data["choices"][0]["message"]["content"].strip()


async def analyse_symbol(symbol: str, analysis_context: str) -> str:
    prompt = (
        f"Analyse {symbol} given the following technical data:\n\n"
        f"{analysis_context}\n\n"
        f"Give a concise trading narrative (3-5 sentences): trend reading, "
        f"key levels, signal direction, and one risk factor."
    )
    return await chat([{"role": "user", "content": prompt}])
