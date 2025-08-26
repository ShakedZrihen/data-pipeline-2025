"""Product-related data models."""
from typing import Optional
from dataclasses import dataclass


@dataclass
class Product:
    """Product data model."""
    item_code: str
    item_name: str
    store_id: str
    chain_id: str
    company_name: Optional[str] = None
    store_city: Optional[str] = None
    store_address: Optional[str] = None
    qty_price: Optional[float] = None


@dataclass
class Promotion:
    """Promotion data model."""
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


@dataclass
class Store:
    """Store data model."""
    store_id: str
    chain_id: str
    store_address: Optional[str] = None
