from pydantic import BaseModel, Field, field_validator
from typing import List

class Item(BaseModel):
    product: str
    price: float
    unit: str | None = None

    @field_validator("product")
    def not_empty(cls, v):
        if not str(v).strip(): raise ValueError("empty product")
        return v

    @field_validator("price")
    def price_positive(cls, v):
        if v is None or v <= 0: raise ValueError("price must be > 0")
        return v

class Envelope(BaseModel):
    provider: str = Field(min_length=1)
    branch: str = Field(min_length=1)
    type: str = Field(min_length=1)
    timestamp: str
    items: List[Item] = Field(min_length=1)
