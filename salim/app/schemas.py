"""
Pydantic schemas for API request and response models.
These models define the structure of data sent to and from the API.
"""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from decimal import Decimal




class ProductBase(BaseModel):
    """Base product schema with common fields."""
    product_id: int
    barcode: Optional[str] = None
    product_name: str = Field(..., description="Product name in Hebrew")
    brand_name: str = Field(default="Unknown", description="Brand name")
    
    class Config:
        from_attributes = True


class BranchBase(BaseModel):
    """Base branch schema with common fields."""
    branch_id: int
    provider: str = Field(..., description="Supermarket chain name")
    name: str = Field(..., description="Branch name")
    city: str = Field(default="Unknown")
    address: str = Field(default="Unknown")
    
    class Config:
        from_attributes = True


class PriceBase(BaseModel):
    """Base price schema with common fields."""
    price_id: int
    price: Decimal = Field(..., description="Regular price in ILS")
    discount_price: Optional[Decimal] = Field(None, description="Promotional price if available")
    final_price: Decimal = Field(..., description="Final price (discount or regular)")
    ts: datetime = Field(..., description="Timestamp of price record")
    
    class Config:
        from_attributes = True




class SupermarketResponse(BaseModel):
    """Response schema for supermarket/provider."""
    provider: str = Field(..., description="Supermarket chain name")
    branch_count: int = Field(..., description="Number of branches")
    branches: Optional[List[BranchBase]] = None
    
    class Config:
        from_attributes = True


class ProductResponse(BaseModel):
    """Response schema for product with latest price."""
    product_id: int
    barcode: Optional[str] = None
    product_name: str
    brand_name: str
    
    price: Optional[Decimal] = None
    discount_price: Optional[Decimal] = None
    final_price: Optional[Decimal] = None
    currency: str = Field(default="ILS", description="Currency is always ILS")
    last_updated: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class ProductWithBranchResponse(BaseModel):
    """Response schema for product with price and branch info."""
   
    product_id: int
    barcode: Optional[str] = None
    product_name: str
    brand_name: str
   
    price: Decimal
    discount_price: Optional[Decimal] = None
    final_price: Decimal
    currency: str = Field(default="ILS")
    
    branch_id: int
    provider: str
    branch_name: str
    city: str
   
    last_updated: datetime
    
    class Config:
        from_attributes = True


class PriceComparisonResponse(BaseModel):
    """Response schema for price comparison across branches."""
    
    product_id: int
    barcode: str
    product_name: str
    brand_name: str
   
    branch_id: int
    provider: str = Field(..., description="Supermarket chain")
    branch_name: str
    city: str
   
    price: Decimal
    discount_price: Optional[Decimal] = None
    final_price: Decimal
    currency: str = Field(default="ILS")
    savings: Optional[Decimal] = Field(None, description="Savings if on discount")
    
    last_updated: datetime
    
    class Config:
        from_attributes = True




class ProductSearchRequest(BaseModel):
    """Request schema for product search."""
    name: Optional[str] = Field(None, description="Search in product name")
    brand: Optional[str] = Field(None, description="Filter by brand")
    min_price: Optional[float] = Field(None, ge=0, description="Minimum price")
    max_price: Optional[float] = Field(None, ge=0, description="Maximum price")
    provider: Optional[str] = Field(None, description="Filter by supermarket chain")
    has_discount: Optional[bool] = Field(None, description="Only products with discounts")
    limit: int = Field(100, ge=1, le=500, description="Maximum results to return")
    offset: int = Field(0, ge=0, description="Offset for pagination")




class HealthResponse(BaseModel):
    """Response schema for health check."""
    status: str
    service: str
    version: str
    database_connected: bool
    table_counts: Optional[dict] = None
    timestamp: datetime = Field(default_factory=datetime.now)
