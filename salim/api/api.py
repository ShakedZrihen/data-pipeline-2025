from fastapi import FastAPI, Query, Path
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

app = FastAPI(title="Supermarket Price API", version="1.0.0")

# -------------------------
# Response Models
# -------------------------
class SupermarketResponse(BaseModel):
    supermarket_id: int
    name: str
    branch_name: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    created_at: datetime


class ProductResponse(BaseModel):
    product_id: int
    supermarket_id: int
    barcode: str
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    size_value: float
    size_unit: str
    price: float
    currency: str = "ILS"
    promo_price: Optional[float] = None
    promo_text: Optional[str] = None
    in_stock: bool
    collected_at: datetime


class PriceComparisonResponse(ProductResponse):
    supermarket_name: str
    savings: float


# -------------------------
# Dummy Data (only Yohananof + Super-Pharm)
# -------------------------
supermarkets = [
    SupermarketResponse(
        supermarket_id=1,
        name="Yohananof",
        website="https://www.yohananof.co.il/",
        created_at=datetime.utcnow(),
    ),
    SupermarketResponse(
        supermarket_id=2,
        name="Super-Pharm",
        website="https://shop.super-pharm.co.il/",
        created_at=datetime.utcnow(),
    ),
]

products = [
    # Yohananof products
    ProductResponse(
        product_id=1,
        supermarket_id=1,
        barcode="7290000000001",
        canonical_name="חלב 3% 1 ליטר",
        brand="תנובה",
        category="חלב ומוצריו",
        size_value=1.0,
        size_unit="ליטר",
        price=5.34,
        in_stock=True,
        collected_at=datetime.utcnow(),
    ),
    ProductResponse(
        product_id=2,
        supermarket_id=1,
        barcode="7290000001112",
        canonical_name="במבה 80 גרם",
        brand="אוסם",
        category="חטיפים",
        size_value=80,
        size_unit="גרם",
        price=4.90,
        promo_price=3.90,
        promo_text="מבצע 2 ב־7",
        in_stock=True,
        collected_at=datetime.utcnow(),
    ),
    # Super-Pharm products
    ProductResponse(
        product_id=3,
        supermarket_id=2,
        barcode="7290000000001",
        canonical_name="חלב 3% 1 ליטר",
        brand="תנובה",
        category="חלב ומוצריו",
        size_value=1.0,
        size_unit="ליטר",
        price=5.80,
        in_stock=True,
        collected_at=datetime.utcnow(),
    ),
    ProductResponse(
        product_id=4,
        supermarket_id=2,
        barcode="7290000002223",
        canonical_name="שמפו דאב 750 מ\"ל",
        brand="Dove",
        category="טואלטיקה",
        size_value=750,
        size_unit="מ\"ל",
        price=19.90,
        in_stock=True,
        collected_at=datetime.utcnow(),
    ),
]

# -------------------------
# Routes: Supermarkets
# -------------------------
@app.get("/supermarkets", response_model=List[SupermarketResponse], tags=["supermarkets"])
def get_supermarkets():
    return supermarkets


@app.get("/supermarkets/{supermarket_id}", response_model=SupermarketResponse, tags=["supermarkets"])
def get_supermarket(supermarket_id: int = Path(..., description="Supermarket ID")):
    for s in supermarkets:
        if s.supermarket_id == supermarket_id:
            return s
    return {"error": "Supermarket not found"}


@app.get("/supermarkets/{supermarket_id}/products", response_model=List[ProductResponse], tags=["supermarkets"])
def get_supermarket_products(
    supermarket_id: int,
    search: Optional[str] = Query(None, description="Search in product names")
):
    return [
        p for p in products
        if p.supermarket_id == supermarket_id and (search is None or search in p.canonical_name)
    ]


# -------------------------
# Routes: Products
# -------------------------
@app.get("/products", response_model=List[ProductResponse], tags=["products"])
def get_products(
    name: Optional[str] = Query(None, alias="q"),
    promo: Optional[bool] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    supermarket_id: Optional[int] = None,
):
    results = products
    if name:
        results = [p for p in results if name in p.canonical_name]
    if promo:
        results = [p for p in results if p.promo_price is not None]
    if min_price:
        results = [p for p in results if p.price >= min_price]
    if max_price:
        results = [p for p in results if p.price <= max_price]
    if supermarket_id:
        results = [p for p in results if p.supermarket_id == supermarket_id]
    return results


@app.get("/products/barcode/{barcode}", response_model=List[PriceComparisonResponse], tags=["products"])
def get_products_by_barcode(barcode: str):
    results = [p for p in products if p.barcode == barcode]
    if not results:
        return []
    min_price = min(p.price for p in results)
    return [
        PriceComparisonResponse(
            **p.dict(),
            supermarket_name=next(s.name for s in supermarkets if s.supermarket_id == p.supermarket_id),
            savings=round(p.price - min_price, 2)
        )
        for p in results
    ]
