from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from decimal import Decimal

class SupermarketResponse(BaseModel):
    supermarket_id: int
    name: str
    branch_name: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True

class ProductResponse(BaseModel):
    product_id: int
    supermarket_id: int
    barcode: str
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    size_value: Optional[Decimal] = None
    size_unit: Optional[str] = None
    price: Decimal
    currency: str = "ILS"
    list_price: Optional[Decimal] = None
    promo_price: Optional[Decimal] = None
    promo_text: Optional[str] = None
    loyalty_only: bool = False
    in_stock: Optional[bool] = None
    collected_at: datetime
    source: Optional[str] = None
    raw_hash: Optional[str] = None

    class Config:
        from_attributes = True

class PriceComparisonResponse(BaseModel):
    product_id: int  # Database row ID
    supermarket_id: int
    supermarket_name: str
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    barcode: str
    price: Decimal
    promo_price: Optional[Decimal] = None
    promo_text: Optional[str] = None
    size_value: Optional[Decimal] = None
    size_unit: Optional[str] = None
    in_stock: Optional[bool] = None
    savings: Optional[Decimal] = None

    class Config:
        from_attributes = True

class ProductSearchParams(BaseModel):
    search: Optional[str] = None
    category: Optional[str] = None
    brand: Optional[str] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    supermarket_id: Optional[int] = None
    limit: int = 100
    offset: int = 0

    class Config:
        from_attributes = True

class LowestPriceResponse(BaseModel):
    product_id: int  # Database row ID
    supermarket_id: int
    supermarket_name: str
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    barcode: str
    price: Decimal
    promo_price: Optional[Decimal] = None
    effective_price: Decimal  # The actual lowest price (promo_price if available, otherwise price)
    savings_percent: Optional[float] = None  # How much cheaper compared to highest price

    class Config:
        from_attributes = True

class PriceHistoryEntry(BaseModel):
    product_id: int  # Database row ID
    date: datetime
    price: Decimal
    promo_price: Optional[Decimal] = None
    effective_price: Decimal
    supermarket_name: str
    
    class Config:
        from_attributes = True

class PriceHistoryResponse(BaseModel):
    barcode: str
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    price_history: List[PriceHistoryEntry]
    current_lowest_price: Decimal
    current_highest_price: Decimal
    price_trend: str  # "increasing", "decreasing", "stable"
    
    class Config:
        from_attributes = True