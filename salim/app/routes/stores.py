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
def get_store_items(store_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 
            i.item_id,
            s.chain_id AS supermarket_name,
            st.store_name,
            i.code,
            i.item_name AS name,
            i.brand,
            i.unit,
            i.quantity AS qty,
            i.price AS unit_price,
            i.regular_price,
            i.promo_price,
            i.promo_start,
            i.promo_end
            FROM items i
            JOIN stores st ON i.store_id = st.store_id
            JOIN supermarkets s ON st.chain_id = s.chain_id
            WHERE i.store_id = %s
        """, (store_id,))
        items = cur.fetchall()
    if not items:
        raise HTTPException(status_code=404, detail="Store not found or no items available")
    return items