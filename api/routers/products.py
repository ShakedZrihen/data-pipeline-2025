from fastapi import APIRouter, Depends, Query
from ..db import get_conn, fetch_all
from ..schemas import Product, PriceComparison
import sqlite3

router = APIRouter(prefix="/products", tags=["products"])

def _conn():
    try:
        conn = get_conn()
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass

@router.get("", response_model=list[Product])
def search_products(
    name: str | None = Query(default=None, alias="q"),
    min_price: float | None = None,
    max_price: float | None = None,
    provider: str | None = None,
    branch: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    conn: sqlite3.Connection = Depends(_conn),
):
    where = ["1=1"]
    params: list = []
    if name:
        where.append("i.product LIKE ?")
        params.append(f"%{name}%")
    if min_price is not None:
        where.append("i.price >= ?")
        params.append(min_price)
    if max_price is not None:
        where.append("i.price <= ?")
        params.append(max_price)
    if provider:
        where.append("m.provider = ?")
        params.append(provider)
    if branch:
        where.append("m.branch = ?")
        params.append(branch)

    sql = f"""
    SELECT i.message_id, i.product, i.price, i.unit,
           m.provider, m.branch, m.ts_iso
    FROM items i
    JOIN messages m ON m.id = i.message_id
    WHERE {' AND '.join(where)}
    ORDER BY m.ts_iso DESC, i.product
    LIMIT ?
    """
    params.append(limit)
    return fetch_all(conn, sql, tuple(params))

@router.get("/compare", response_model=list[PriceComparison])
def compare_by_name(
    name: str = Query(..., description="שם מוצר להשוואה"),
    conn: sqlite3.Connection = Depends(_conn),
):
    sql = """
    WITH last_per_shop AS (
      SELECT m.provider, m.branch, MAX(m.ts_iso) AS max_ts
      FROM messages m
      GROUP BY m.provider, m.branch
    )
    SELECT m.provider, m.branch, i.product, i.price, i.unit, m.ts_iso
    FROM items i
    JOIN messages m ON m.id=i.message_id
    JOIN last_per_shop l ON l.provider=m.provider AND l.branch=m.branch AND l.max_ts=m.ts_iso
    WHERE i.product LIKE ?
    ORDER BY i.price IS NULL, i.price ASC, m.provider, m.branch
    """
    return fetch_all(conn, sql, (f"%{name}%",))
