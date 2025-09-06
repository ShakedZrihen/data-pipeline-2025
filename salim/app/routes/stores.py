from fastapi import APIRouter , HTTPException
from typing import List
from db.db import get_conn
from models.models import StoreResponse , ItemResponse

router = APIRouter(prefix="/stores", tags=["stores"])

@router.get("", response_model=List[StoreResponse])
def list_stores():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT store_id, chain_id, store_name, address, city FROM stores ORDER BY store_name""")
        rows = cur.fetchall()
    return rows

@router.get("/{store_id}/items", response_model=List[ItemResponse])
def get_store_items(store_id: str , limit: int = 100):  

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                s.store_name,
                i.code,
                i.name,
                i.brand,
                i.unit,
                i.qty,
                i.unit_price,
                i.regular_price,
                i.promo_price,
                i.promo_start,
                i.promo_end
            FROM items AS i
            JOIN stores AS s ON s.store_id = i.store_id
            WHERE i.store_id = %s
            ORDER BY i.code
        """, (store_id,))
        rows = cur.fetchall()
        if limit:
            rows = rows[:limit]
    if not rows:
        raise HTTPException(status_code=404, detail="Item not found")
    return rows


@router.get("/{store_id}/items/{item_code}", response_model=ItemResponse)
def get_store_item(store_id: str, item_code: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                s.store_name,
                i.code,
                i.name,
                i.brand,
                i.unit,
                i.qty,
                i.unit_price,
                i.regular_price,
                i.promo_price,
                i.promo_start,
                i.promo_end
            FROM items AS i
            JOIN stores AS s ON s.store_id = i.store_id
            WHERE i.store_id = %s AND i.code = %s
        """, (store_id, item_code))
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Item not found")

    return row