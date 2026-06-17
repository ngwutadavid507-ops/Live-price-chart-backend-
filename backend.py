from fastapi import WebSocket, WebSocketDisconnect
import aiohttp
import asyncio
import time
import os

CMC_API_KEY = os.getenv("CMC_API_KEY")

CMC_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    headers = {
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }

    try:
        while True:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    CMC_URL,
                    headers=headers,
                    params={
                        "limit": 50,
                        "convert": "USD"
                    }
                ) as r:

                    data = await r.json()

            coins = data.get("data", [])

            # sort top movers
            top = sorted(
                coins,
                key=lambda x: x["quote"]["USD"]["percent_change_24h"],
                reverse=True
            )[:20]

            await ws.send_json({
                "type": "hot",
                "data": [
                    {
                        "symbol": c["symbol"] + "USDT",
                        "price": c["quote"]["USD"]["price"],
                        "change": c["quote"]["USD"]["percent_change_24h"],
                        "volume": c["quote"]["USD"]["volume_24h"]
                    }
                    for c in top
                ],
                "time": time.time()
            })

            await asyncio.sleep(2)

    except WebSocketDisconnect:
        print("Client disconnected")
