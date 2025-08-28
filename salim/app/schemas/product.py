"""Product-related Pydantic schemas for request/response validation."""
from typing import List, Optional
from pydantic import BaseModel, Field


class ProductResponse(BaseModel):
    """Response model for product information."""
    item_code: str = Field(..., description="Product barcode")
    item_name: str = Field(..., description="Product name")
    store_id: str = Field(..., description="Store branch identifier")
    chain_id: str = Field(..., description="Supermarket chain identifier")
    has_promotion: bool = Field(..., description="Whether product has active promotion")
    discount_rate: float = Field(..., description="Discount rate (0.0 if no promotion)")
    price: float = Field(..., description="Current product price")
    store_address: Optional[str] = Field(None, description="Store branch address")

    class Config:
        json_schema_extra = {
            "example": {
                "item_code": "7290000123456",
                "item_name": "מוצר לדוגמה",
                "store_id": "001",
                "chain_id": "7290027600007",
                "has_promotion": True,
                "discount_rate": 0.15,
                "price": 12.90,
                "store_address": "רחוב הדגמה 123, תל אביב"
            }
        }


class ProductListResponse(BaseModel):
    """Response model for list of products."""
    products: List[ProductResponse]
    total: int = Field(..., description="Total number of products found")

    class Config:
        json_schema_extra = {
            "example": {
                "products": [
                    {
                        "item_code": "7290000123456",
                        "item_name": "מוצר לדוגמה",
                        "store_id": "001",
                        "chain_id": "7290027600007",
                        "has_promotion": True,
                        "discount_rate": 0.15,
                        "price": 12.90,
                        "store_address": "רחוב הדגמה 123, תל אביב"
                    }
                ],
                "total": 1
            }
        }


class PromotionResponse(BaseModel):
    """Response model for promotion information."""
    promotion_id: str
    promotion_description: Optional[str] = None
    discount_rate: Optional[float] = None
    reward_type: Optional[str] = None
    promotion_start_date: Optional[str] = None
    promotion_end_date: Optional[str] = None
    additional_is_active: Optional[bool] = None
    item_code: Optional[str] = None
    chain_id: Optional[str] = None
    store_id: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "promotion_id": "PROMO123",
                "promotion_description": "הנחה של 15% על המוצר",
                "discount_rate": 0.15,
                "reward_type": "PERCENTAGE",
                "promotion_start_date": "2025-08-01",
                "promotion_end_date": "2025-08-31",
                "additional_is_active": True,
                "item_code": "7290000123456",
                "chain_id": "7290027600007",
                "store_id": "001"
            }
        }


class PromotionListResponse(BaseModel):
    """Response model for list of promotions."""
    promotions: List[PromotionResponse]
    total: int = Field(..., description="Total number of promotions found")


class StoreResponse(BaseModel):
    """Response model for store branch information."""
    store_id: str = Field(..., description="Store branch identifier")
    chain_id: str = Field(..., description="Supermarket chain identifier")
    store_address: Optional[str] = Field(None, description="Store branch address")

    class Config:
        json_schema_extra = {
            "example": {
                "store_id": "001",
                "chain_id": "7290027600007",
                "store_address": "רחוב הדגמה 123, תל אביב"
            }
        }


class StoreListResponse(BaseModel):
    """Response model for list of stores."""
    stores: List[StoreResponse]
    total: int = Field(..., description="Total number of stores found")


class SupermarketResponse(BaseModel):
    """Response model for supermarket chain information."""
    supermarket_id: str = Field(..., description="Supermarket chain identifier")
    chain_id: str = Field(..., description="Chain identifier (same as supermarket_id)")
    company_name: Optional[str] = Field(None, description="Company/chain name")

    class Config:
        json_schema_extra = {
            "example": {
                "supermarket_id": "7290027600007",
                "chain_id": "7290027600007",
                "company_name": "רשת סופר פארם"
            }
        }


class SupermarketListResponse(BaseModel):
    """Response model for list of supermarkets."""
    supermarkets: List[SupermarketResponse]
    total: int = Field(..., description="Total number of supermarkets found")


class ErrorResponse(BaseModel):
    """Response model for errors."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Additional error details")
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "Item not found",
                "detail": "No products found with the specified barcode"
            }
        }
