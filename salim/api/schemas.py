from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from uuid import UUID

class SupermarketResponse(BaseModel):
    id: UUID
    address: Optional[str] = None
    provider: Optional[str] = None
    branch_number: Optional[int] = None
    branch_name: Optional[str] = None

    class Config:
        from_attributes = True

class ProductResponse(BaseModel):
    id: UUID
    branch_number: Optional[int] = None
    name: Optional[str] = None
    barcode: Optional[str] = None
    updated_at: Optional[datetime] = None
    super_id: Optional[UUID] = None
    price: Optional[float] = None

    class Config:
        from_attributes = True

class PriceComparisonResponse(BaseModel):
    product_id: str
    supermarket_id: str
    supermarket_name: Optional[str] = None
    name: Optional[str] = None
    barcode: Optional[str] = None
    price: Optional[float] = None

    class Config:
        from_attributes = True

class LowestPriceResponse(BaseModel):
    product_id: str
    supermarket_id: str
    supermarket_name: Optional[str] = None
    name: Optional[str] = None
    barcode: Optional[str] = None
    price: Optional[float] = None
    effective_price: Optional[float] = None
    savings_percent: Optional[float] = None

    class Config:
        from_attributes = True

class PriceHistoryEntry(BaseModel):
    product_id: str
    date: datetime
    price: Optional[float] = None
    effective_price: Optional[float] = None
    supermarket_name: Optional[str] = None

    class Config:
        from_attributes = True

class PriceHistoryResponse(BaseModel):
    barcode: str
    canonical_name: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    price_history: List[PriceHistoryEntry]
    current_lowest_price: Optional[float] = None
    current_highest_price: Optional[float] = None
    price_trend: Optional[str] = None

    class Config:
        from_attributes = True