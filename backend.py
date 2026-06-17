from fastapi import FastAPI
import aiohttp
import os

app = FastAPI()

CMC_API_KEY = os.getenv("CMC_API_KEY")  # set this on Render

@app.get("/cmc-test")
async def cmc_test():
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

    headers = {
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }

    params = {
        "limit": 10,
        "convert": "USD"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as r:
            data = await r.text()

            return {
                "status_code": r.status,
                "raw_preview": data[:1000]
            }
