from pydantic import BaseModel
from typing import Optional


class ChatMessage(BaseModel):
    role:    str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = []
    symbol:  Optional[str] = None


class ChatResponse(BaseModel):
    reply:  str
    symbol: Optional[str] = None


class AnalyseRequest(BaseModel):
    symbol:  str
    context: Optional[str] = None


class AnalyseResponse(BaseModel):
    symbol:     str
    narrative:  str
    signal:     str
    confidence: float
