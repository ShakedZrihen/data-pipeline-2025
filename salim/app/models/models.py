from pydantic import BaseModel
from typing import Optional
from datetime import datetime



class SupermarketResponse(BaseModel):
    chain_id: str
    name: str

class StoreResponse(BaseModel):
    store_id: str
    chain_id: str
    store_name: str
    address: Optional[str] = None
    city: Optional[str] = None

class ProductResponse(BaseModel):
    code: str
    chain_id: str
    store_id: str
    name: Optional[str] = None
    brand: Optional[str] = None
    unit: Optional[str] = None
    qty: Optional[float] = None
    unit_price: Optional[float] = None
    price: Optional[float] = None            
    regular_price: Optional[float] = None
    promo_price: Optional[float] = None
    promo_start: Optional[datetime] = None
    promo_end: Optional[datetime] = None
    currency: Optional[str] = "ILS"          

class PriceComparisonResponse(BaseModel):
    code: str
    chain_id: str
    store_id: str
    supermarket_name: str
    name: Optional[str] = None
    brand: Optional[str] = None
    unit: Optional[str] = None
    qty: Optional[float] = None
    price: Optional[float] = None
    promo_price: Optional[float] = None
    savings: Optional[float] = None

class ItemResponse(BaseModel):
    store_name: Optional[str] = None
    code: str
    name: str
    brand: Optional[str] = None
    unit: Optional[str] = None
    qty: Optional[float] = None
    unit_price: Optional[float] = None
    regular_price: Optional[float] = None
    promo_price: Optional[float] = None
    promo_start: Optional[datetime] = None
    promo_end: Optional[datetime] = None




# CREATE TABLE IF NOT EXISTS items (
#   chain_id      TEXT NOT NULL REFERENCES supermarkets(chain_id),
#   store_id      TEXT NOT NULL REFERENCES stores(store_id),
#   code          TEXT NOT NULL,

#   name          TEXT,
#   brand         TEXT,
#   unit          TEXT,
#   qty           NUMERIC(12,3),
#   unit_price    NUMERIC(12,4),

#   regular_price NUMERIC(12,2),

#   promo_price   NUMERIC(12,2),
#   promo_start   TIMESTAMPTZ,
#   promo_end     TIMESTAMPTZ,

#   PRIMARY KEY (chain_id, store_id, code)
# );

# COMMIT;
