from pydantic import BaseModel
from typing import Optional


class JournalEntry(BaseModel):
    id:     Optional[int] = None
    date:   str
    pair:   str
    side:   str
    entry:  str
    exit:   Optional[str] = "—"
    pnl:    float
    status: str
    tags:   list[str] = []
    note:   str = ""


class JournalCreateRequest(BaseModel):
    pair:  str
    side:  str
    entry: str
    exit:  Optional[str] = None
    pnl:   float
    tags:  list[str] = []
    note:  str = ""
