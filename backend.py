from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import aiohttp

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {
        "status": "running",
        "version": "debug-v1"
    }

@app.get("/symbols")
async def symbols():
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=20) as resp:

                text = await resp.text()

                return {
                    "success": True,
                    "http_status": resp.status,
                    "sample": text[:500]
                }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
                }
