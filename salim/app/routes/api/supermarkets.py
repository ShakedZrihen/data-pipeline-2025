from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

from enricher.db_schema import DATABASE_URL, Supermarket, Product, ProductPrice

router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in environment")
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, future=True)

class SupermarketResponse(BaseModel):
    supermarket_id: int
    branch_name: str
    name: str
    city: str
    address: str
    website: Optional[str] = None

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

@router.get("/", response_model=List[SupermarketResponse])
def list_supermarkets():
    with SessionLocal() as session:
        rows = session.execute(select(Supermarket)).scalars().all()
        return [
            SupermarketResponse(
                supermarket_id=r.supermarket_id,
                branch_name=r.branch_name,
                name=r.name,
                city=r.city,
                address=r.address,
                website=r.website,
            )
            for r in rows
        ]

@router.get("/{supermarket_id}", response_model=SupermarketResponse)
def get_supermarket(supermarket_id: int):
    with SessionLocal() as session:
        row = session.get(Supermarket, supermarket_id)
        if not row:
            raise HTTPException(status_code=404, detail="Supermarket not found")
        return SupermarketResponse(
            supermarket_id=row.supermarket_id,
            branch_name=row.branch_name,
            name=row.name,
            city=row.city,
            address=row.address,
            website=row.website,
        )

@router.get("/{supermarket_id}/products", response_model=List[ProductResponse])
def get_supermarket_products(
    supermarket_id: int,
    search: Optional[str] = Query(None, description="Search in product names"),
    limit: int = Query(100, ge=1, le=1000),
):
    with SessionLocal() as session:

        subq = (
            select(
                ProductPrice.product_id,
                func.max(ProductPrice.collected_at).label("max_collected")
            )
            .where(ProductPrice.supermarket_id == supermarket_id)
            .group_by(ProductPrice.product_id)
            .subquery()
        )

        stmt = (
            select(ProductPrice, Product)
            .join(subq, (ProductPrice.product_id == subq.c.product_id) & (ProductPrice.collected_at == subq.c.max_collected))
            .join(Product, Product.product_id == ProductPrice.product_id)
            .where(ProductPrice.supermarket_id == supermarket_id)
            .limit(limit)
        )

        if search:
            stmt = stmt.filter(Product.canonical_name.ilike(f"%{search}%"))

        rows = session.execute(stmt).all()
        results = []
        for pp_row, prod in rows:
            results.append(
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
        return results