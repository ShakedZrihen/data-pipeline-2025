from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..database import get_db
from ..schemas import SupermarketResponse
from .utils import seed_supermarkets_if_empty
from typing import List

router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])

@router.get("", response_model=List[SupermarketResponse])
def list_supermarkets(db: Session = Depends(get_db)):
    seed_supermarkets_if_empty(db)
    rows = db.execute(text("""
      SELECT supermarket_id, name, branch_name, city, address, website,
             to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"+00:00"') as created_at
      FROM supermarket
      ORDER BY supermarket_id
    """)).mappings().all()
    return rows

@router.get("/{supermarket_id}", response_model=SupermarketResponse)
def get_supermarket(supermarket_id: int, db: Session = Depends(get_db)):
    row = db.execute(text("""
      SELECT supermarket_id, name, branch_name, city, address, website,
             to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"+00:00"') as created_at
      FROM supermarket WHERE supermarket_id=:id
    """), {"id": supermarket_id}).mappings().first()
    if not row:
        raise HTTPException(404, "supermarket not found")
    return row

@router.get("/{supermarket_id}/products")
def products_of_market(supermarket_id: int, search: str | None = None, db: Session = Depends(get_db)):
    sql = """
    SELECT vp.provider, vp.branch_code, vp.product_code, vp.product_name,
           vp.unit, vp.price::float AS price,
           vp.ts AT TIME ZONE 'UTC' AS collected_at
    FROM v_latest_price vp
    JOIN supermarket s USING (provider, branch_code)
    WHERE s.supermarket_id = :id
    """
    params = {"id": supermarket_id}
    if search:
        sql += " AND vp.product_name ILIKE :q"
        params["q"] = f"%{search}%"

    sql += " ORDER BY vp.product_name LIMIT 200"

    rows = db.execute(text(sql), params).mappings().all()

    out = []
    import zlib
    for r in rows:
        pid = zlib.crc32(f"{r['provider']}#{r['branch_code']}#{r['product_code']}".encode()) & 0xffffffff
        out.append({
            "product_id": pid,
            "supermarket_id": supermarket_id,
            "barcode": r["product_code"],
            "canonical_name": r["product_name"],
            "brand": None,
            "category": None,
            "size_value": None,
            "size_unit": r["unit"],
            "price": r["price"],
            "currency": "ILS",
            "promo_price": None,
            "promo_text": None,
            "in_stock": True,
            "collected_at": r["collected_at"].isoformat(),
        })
    return out