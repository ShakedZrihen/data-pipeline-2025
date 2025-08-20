from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

class Item(BaseModel):
    product: str
    price: float
    unit: Optional[str] = None

class Envelope(BaseModel):
    provider: str
    branch: str
    type: str = Field(alias="type")
    timestamp: datetime
    items: List[Item]
