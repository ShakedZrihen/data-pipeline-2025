from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class SupermarketResponse(BaseModel):
    supermarket_id: int
    name: str
    branch_name: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    created_at: datetime

class ProductResponse(BaseModel):
    product_id: int = Field(..., example=24806503)
    supermarket_id: int = Field(..., example=1)
    barcode: str = Field(..., example="7290000000001")
    canonical_name: str = Field(..., example="חלב 3% 1 ליטר")
    brand: Optional[str] = None
    category: Optional[str] = None
    size_value: Optional[float] = None
    size_unit: Optional[str] = Field(None, example="ליטר")
    price: float = Field(..., example=5.34)
    currency: str = Field(..., example="ILS")
    promo_price: Optional[float] = None
    promo_text: Optional[str] = None
    in_stock: bool = True
    collected_at: Optional[datetime] = None

class PriceComparisonResponse(BaseModel):
    product_id: int
    supermarket_id: int
    supermarket_name: str
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    barcode: str
    price: float
    promo_price: Optional[float] = None
    promo_text: Optional[str] = None
    size_value: Optional[float] = None
    size_unit: Optional[str] = None
    in_stock: bool = True
    savings: float