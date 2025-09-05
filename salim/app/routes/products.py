from fastapi import APIRouter, HTTPException, Query
from typing import List
from db.db import get_conn
from models.models import ProductResponse, PriceComparisonResponse

router = APIRouter(prefix="/products", tags=["products"])

@router.get("", response_model=List[ProductResponse])
def search_products(
    q: str | None = Query(None, description="Filter by product name "),
    promo: bool | None = Query(None, description="Only promo items (bool)"),
    min_price: float | None = None,
    max_price: float | None = None,
    limit: int = 100
):
    where = []
    params: list = []

    if q:
        where.append("(ic.name ILIKE %s OR ic.brand ILIKE %s OR ic.code ILIKE %s)")
        like = f"%{q}%"
        params += [like, like, like]

    if promo is True:
        where.append("ic.promo_price IS NOT NULL")
    elif promo is False:
        where.append("ic.promo_price IS NULL")

    if min_price is not None:
        where.append("COALESCE(ic.promo_price, ic.regular_price) >= %s")
        params.append(min_price)
    if max_price is not None:
        where.append("COALESCE(ic.promo_price, ic.regular_price) <= %s")
        params.append(max_price)

    sql = f"""
        SELECT
          ic.code, ic.chain_id, ic.store_id, ic.name, ic.brand, ic.unit, ic.qty, ic.unit_price,
          ic.regular_price, ic.promo_price, ic.promo_start, ic.promo_end
        FROM items ic
        {"WHERE " + " AND ".join(where) if where else ""}
        ORDER BY COALESCE(ic.promo_price, ic.regular_price) NULLS LAST
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

    def to_price(r):
        price = r["promo_price"] if r["promo_price"] is not None else r["regular_price"]
        return None if price is None else float(price)

    return [
        {
            **r,
            "qty": float(r["qty"]) if r["qty"] is not None else None,
            "unit_price": float(r["unit_price"]) if r["unit_price"] is not None else None,
            "regular_price": float(r["regular_price"]) if r["regular_price"] is not None else None,
            "promo_price": float(r["promo_price"]) if r["promo_price"] is not None else None,
            "price": to_price(r),
        }
        for r in rows
    ]

@router.get("/code/{code}", response_model=List[PriceComparisonResponse])
def compare_by_code(code: str):
    """
    Return all stores/supermarkets carrying the same code (barcode),
    sorted by effective price (promo or regular).
    """
    sql = """
        SELECT
          ic.code,
          ic.chain_id,
          ic.store_id,
          s.chain_name AS supermarket_name,
          ic.name,
          ic.brand,
          ic.unit,
          ic.qty,
          ic.regular_price,
          ic.promo_price
        FROM items ic
        JOIN supermarkets s ON s.chain_id = ic.chain_id
        WHERE ic.code = %s
        ORDER BY COALESCE(ic.promo_price, ic.regular_price) ASC NULLS LAST
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (code,))
        rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No items found for this code")

    def eff_price(r):
        return r["promo_price"] if r["promo_price"] is not None else r["regular_price"]

    cheapest = None
    for r in rows:
        p = eff_price(r)
        if p is not None and (cheapest is None or p < cheapest):
            cheapest = p

    out = []
    for r in rows:
        p = eff_price(r)
        savings = None
        if p is not None and cheapest is not None:
            savings = float(p) - float(cheapest)
        out.append({
            "code": r["code"],
            "chain_id": r["chain_id"],
            "store_id": r["store_id"],
            "supermarket_name": r["supermarket_name"],
            "name": r["name"],
            "brand": r["brand"],
            "unit": r["unit"],
            "qty": float(r["qty"]) if r["qty"] is not None else None,
            "price": float(r["regular_price"]) if r["regular_price"] is not None else None,
            "promo_price": float(r["promo_price"]) if r["promo_price"] is not None else None,
            "savings": float(savings) if savings is not None else None
        })
    return out
