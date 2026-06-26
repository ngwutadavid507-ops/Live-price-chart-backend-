"""
/news — crypto news feed with sentiment.
Powers: news.js + dashboard news preview.
"""

from fastapi import APIRouter, Query
from services.news_feed import get_news

router = APIRouter()


@router.get("/feed")
async def news_feed(
    filter_:    str = Query("hot", alias="filter"),
    currencies: str = Query(""),
):
    articles = await get_news(
        filter_=filter_,
        currencies=currencies if currencies else None,
    )
    return {"count": len(articles), "articles": articles}
