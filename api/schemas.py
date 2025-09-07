from typing import Optional, List
from pydantic import BaseModel

class Supermarket(BaseModel):
    provider: str
    branch: str
    name: str | None = None
    items_messages: int | None = None

class Product(BaseModel):
    message_id: int
    product: str
    price: Optional[float] = None
    unit: Optional[str] = None
    provider: str
    branch: str
    ts_iso: str

class PriceComparison(BaseModel):
    provider: str
    branch: str
    product: str
    price: Optional[float] = None
    unit: Optional[str] = None
    ts_iso: str
