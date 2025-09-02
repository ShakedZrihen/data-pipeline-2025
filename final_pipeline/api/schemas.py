from typing import Optional, List
from pydantic import BaseModel

class SupermarketResponse(BaseModel):
    supermarket_id: int
    name: str
    branch_name: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    created_at: Optional[str] = None

class ProductResponse(BaseModel):
    product_id: int
    supermarket_id: int
    barcode: Optional[str] = None
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    size_value: Optional[float] = None
    size_unit: Optional[str] = None
    price: float
    currency: str
    promo_price: Optional[float] = None
    promo_text: Optional[str] = None
    in_stock: bool
    collected_at: Optional[str] = None

class PriceComparisonResponse(BaseModel):
    product_id: int
    supermarket_id: int
    supermarket_name: str
    canonical_name: str
    brand: Optional[str]
    category: Optional[str]
    barcode: Optional[str]
    price: float
    promo_price: Optional[float]
    promo_text: Optional[str]
    size_value: Optional[float]
    size_unit: Optional[str]
    in_stock: bool
    savings: float
