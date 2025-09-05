from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class SupermarketResponse(BaseModel):
    supermarket_id: int
    name: str
    branch_name: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    created_at: datetime


class ProductResponse(BaseModel):
    id: int
    provider: str
    branch: str
    product_name: str
    manufacturer: Optional[str] = None
    price: float
    unit: str
    category: Optional[str] = None
    is_promotion: bool
    is_kosher: Optional[bool] = None
    file_timestamp: datetime
    created_at: datetime