from typing import List, Optional
from fastapi import APIRouter, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from enricher.db_schema import DATABASE_URL, Product, ProductPrice

router = APIRouter(prefix="/products", tags=["products"])

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in environment")
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, future=True)

class ProductResponse(BaseModel):
    product_id: int
    supermarket_id: int
    barcode: Optional[str] = None
    canonical_name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    size_value: Optional[float] = None
    size_unit: Optional[str] = None
    price: Optional[float] = None
    promo_price: Optional[float] = None
    promo_text: Optional[str] = None
    in_stock: bool
    collected_at: Optional[str] = None

class PriceComparisonResponse(BaseModel):
    product_id: int
    supermarket_id: int
    barcode: Optional[str]
    price: Optional[float]
    promo_price: Optional[float]
    promo_text: Optional[str]
    in_stock: bool
    collected_at: Optional[str]

@router.get("", response_model=List[ProductResponse])
def search_products(
    name: Optional[str] = Query(None),
    promo: Optional[bool] = Query(None),
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    supermarket_id: Optional[int] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    with SessionLocal() as session:
        print(f"Search products with name={name}, promo={promo}, min_price={min_price}, max_price={max_price}, supermarket_id={supermarket_id}, limit={limit}")
        stmt = select(ProductPrice, Product).join(Product, Product.product_id == ProductPrice.product_id)

        if name:
            stmt = stmt.filter(Product.canonical_name.ilike(f"%{name}%"))
        if promo is True:
            stmt = stmt.filter(ProductPrice.promo_price.isnot(None))
        if min_price is not None:
            stmt = stmt.filter(ProductPrice.price >= min_price)
        if max_price is not None:
            stmt = stmt.filter(ProductPrice.price <= max_price)
        if supermarket_id is not None:
            stmt = stmt.filter(ProductPrice.supermarket_id == supermarket_id)

        stmt = stmt.order_by(ProductPrice.price.asc().nullslast()).limit(limit)

        rows = session.execute(stmt).all()
        resp = []
        for pp_row, prod in rows:
            resp.append(
                ProductResponse(
                    product_id=int(prod.product_id),
                    supermarket_id=int(pp_row.supermarket_id),
                    barcode=getattr(prod, "barcode", None),
                    canonical_name=prod.canonical_name or "",
                    brand=prod.brand,
                    category=prod.category,
                    size_value=float(pp_row.size_value) if pp_row.size_value is not None else None,
                    size_unit=pp_row.size_unit,
                    price=float(pp_row.price) if pp_row.price is not None else None,
                    promo_price=float(pp_row.promo_price) if pp_row.promo_price is not None else None,
                    promo_text=pp_row.promo_text,
                    in_stock=bool(pp_row.in_stock),
                    collected_at=pp_row.collected_at.isoformat() if pp_row.collected_at else None,
                )
            )
        return resp

@router.get("/barcode/{barcode}", response_model=List[PriceComparisonResponse])
def products_by_barcode(barcode: str):
    with SessionLocal() as session:
        stmt = select(ProductPrice, Product).join(Product, Product.product_id == ProductPrice.product_id).where(Product.barcode == barcode)
        stmt = stmt.order_by(ProductPrice.price.asc().nullslast())
        rows = session.execute(stmt).all()
        resp = []
        for pp_row, prod in rows:
            resp.append(
                PriceComparisonResponse(
                    product_id=int(prod.product_id),
                    supermarket_id=int(pp_row.supermarket_id),
                    barcode=getattr(prod, "barcode", None),
                    price=float(pp_row.price) if pp_row.price is not None else None,
                    promo_price=float(pp_row.promo_price) if pp_row.promo_price is not None else None,
                    promo_text=pp_row.promo_text,
                    in_stock=bool(pp_row.in_stock),
                    collected_at=pp_row.collected_at.isoformat() if pp_row.collected_at else None,
                )
            )
        return resp