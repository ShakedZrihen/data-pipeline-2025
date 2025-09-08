from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List
from .products import Product
from ..database import _conn


router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])

class Supermarket(BaseModel):
    id: int
    name: str
    created_at: datetime


@router.get("", response_model=list[Supermarket])
def list_supermarkets():
    con = _conn()
    cur = con.cursor()
    try:
        Query = """SELECT * FROM supermarkets;"""
        cur.execute(Query)
        rows = cur.fetchall()
        return rows
    finally:
        con.close()


@router.get("/{supermarket_id}", response_model=Supermarket, tags=["supermarkets"])
def get_supermarket(supermarket_id: int):
    con = _conn()
    cur = con.cursor()
    try:
        query = "SELECT * FROM supermarkets WHERE id = %s"
        cur.execute(query, (supermarket_id,))
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Supermarket not found")
        return row
    finally:
        con.close()


@router.get("/{supermarket_id}/products", response_model=List[Product], tags=["supermarkets"])
def get_products_for_supermarket(
    supermarket_id: int,
    search: Optional[str] = Query(None, description="Filter by product name")
):
    con = _conn()
    cur = con.cursor()
    try:

        sql = """
            SELECT
                pp.id,
                pp.supermarket_id,
                pp.barcode,
                pp.name,
                pp.brand,
                NULL::text AS category,
                pp.unit_of_measure,
                pp.quantity,
                cp.price AS price,
                CASE WHEN cp.price_type = 'promo' THEN cp.price ELSE NULL END AS promo_price,
                (cp.price_type = 'promo') AS promo,
                b.city,
                b.address,
                'ILS' AS currency,
                pp.created_at          -- <-- keep raw, no COALESCE
            FROM provider_products pp
            LEFT JOIN LATERAL (
                SELECT c1.*
                FROM current_prices c1
                WHERE c1.provider_product_id = pp.id
                ORDER BY c1.effective_at DESC
                LIMIT 1
            ) cp ON TRUE
            LEFT JOIN branches b ON b.id = cp.branch_id
            WHERE pp.supermarket_id = %s
        """

        params = [supermarket_id]

        if search:
            sql += " AND pp.name ILIKE %s"
            params.append(f"%{search}%")

        sql += " ORDER BY pp.id"

        cur.execute(sql, params)
        rows = cur.fetchall()
        return rows
    finally:
        con.close()