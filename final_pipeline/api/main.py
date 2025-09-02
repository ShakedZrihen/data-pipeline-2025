from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from .db import SessionLocal, engine
from .models import Supermarket, Product
from .schemas import SupermarketResponse, ProductResponse, PriceComparisonResponse

app = FastAPI(title="Supermarket Prices API", version="1.0.0", docs_url="/docs", redoc_url="/redoc")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def to_supermarket_response(sm: Supermarket) -> SupermarketResponse:
    return SupermarketResponse(
        supermarket_id=sm.supermarket_id,
        name=sm.name,
        branch_name=sm.branch_name,
        city=sm.city,
        address=sm.address,
        website=sm.website,
        created_at=sm.created_at.isoformat() if sm.created_at else None,
    )

def to_product_response(p: Product) -> ProductResponse:
    return ProductResponse(
        product_id=p.product_id,
        supermarket_id=p.supermarket_id,
        barcode=p.barcode,
        canonical_name=p.canonical_name,
        brand=p.brand,
        category=p.category,
        size_value=p.size_value,
        size_unit=p.size_unit,
        price=p.price,
        currency=p.currency,
        promo_price=p.promo_price,
        promo_text=p.promo_text,
        in_stock=p.in_stock,
        collected_at=p.collected_at.isoformat() if p.collected_at else None,
    )

@app.get("/supermarkets", response_model=list[SupermarketResponse], tags=["supermarkets"])
def get_supermarkets():
    with SessionLocal() as db:
        sms = db.execute(select(Supermarket).order_by(Supermarket.supermarket_id)).scalars().all()
        return [to_supermarket_response(sm) for sm in sms]

@app.get("/supermarkets/{supermarket_id}", response_model=SupermarketResponse, tags=["supermarkets"])
def get_supermarket(supermarket_id: int = Path(..., description="The supermarket ID")):
    with SessionLocal() as db:
        sm = db.get(Supermarket, supermarket_id)
        if not sm:
            raise HTTPException(status_code=404, detail="Supermarket not found")
        return to_supermarket_response(sm)

@app.get("/supermarkets/{supermarket_id}/products", response_model=list[ProductResponse], tags=["supermarkets"])
def get_supermarket_products(supermarket_id: int, search: str | None = Query(None, description="Search in product names")):
    with SessionLocal() as db:
        stmt = select(Product).where(Product.supermarket_id == supermarket_id)
        if search:
            stmt = stmt.where(Product.canonical_name.ilike(f"%{search}%"))
        stmt = stmt.order_by(Product.collected_at.desc().nullslast(), Product.product_id.desc()).limit(200)
        rows = db.execute(stmt).scalars().all()
        return [to_product_response(p) for p in rows]

@app.get("/products", response_model=list[ProductResponse], tags=["products"])
def search_products(
    name: str | None = Query(None, alias="q"),
    promo: bool | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    supermarket_id: int | None = None,
):
    with SessionLocal() as db:
        stmt = select(Product)
        if name:
            stmt = stmt.where(Product.canonical_name.ilike(f"%{name}%"))
        if promo is True:
            stmt = stmt.where(Product.promo_price.isnot(None))
        if min_price is not None:
            stmt = stmt.where(Product.price >= min_price)
        if max_price is not None:
            stmt = stmt.where(Product.price <= max_price)
        if supermarket_id:
            stmt = stmt.where(Product.supermarket_id == supermarket_id)
        stmt = stmt.order_by(Product.collected_at.desc().nullslast(), Product.product_id.desc()).limit(200)
        rows = db.execute(stmt).scalars().all()
        return [to_product_response(p) for p in rows]

@app.get("/products/barcode/{barcode}", response_model=list[PriceComparisonResponse], tags=["products"])
def by_barcode(barcode: str):
    with SessionLocal() as db:
        stmt = select(Product, Supermarket.name).where(Product.barcode == barcode).join(Supermarket, Product.supermarket_id == Supermarket.supermarket_id)
        rows = db.execute(stmt).all()
        if not rows:
            return []
        prices = []
        min_price = min(r[0].price for r in rows)
        for prod, sm_name in rows:
            prices.append(PriceComparisonResponse(
                product_id=prod.product_id,
                supermarket_id=prod.supermarket_id,
                supermarket_name=sm_name,
                canonical_name=prod.canonical_name,
                brand=prod.brand,
                category=prod.category,
                barcode=prod.barcode,
                price=prod.price,
                promo_price=prod.promo_price,
                promo_text=prod.promo_text,
                size_value=prod.size_value,
                size_unit=prod.size_unit,
                in_stock=prod.in_stock,
                savings=round(prod.price - min_price, 2),
            ))
        prices.sort(key=lambda x: x.price)
        return prices
