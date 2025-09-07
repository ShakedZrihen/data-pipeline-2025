from fastapi import APIRouter, Query
import sqlite3, os

router = APIRouter(prefix="/enriched", tags=["enriched"])
DB_PATH = os.environ.get("DB_PATH", "data/prices.db")

@router.get("")
def search_enriched(q: str = Query(None), limit: int = 50):
    with sqlite3.connect(DB_PATH) as con:
        cur = con.cursor()
        if q:
            cur.execute("""
                SELECT message_id, product_norm, price, unit_norm, meta_json
                FROM enriched_items
                WHERE product_norm LIKE ?
                LIMIT ?
            """, (f"%{q.lower()}%", limit))
        else:
            cur.execute("""
                SELECT message_id, product_norm, price, unit_norm, meta_json
                FROM enriched_items
                ORDER BY id DESC
                LIMIT ?
            """, (limit,))
        rows = cur.fetchall()
    return [
        {
            "message_id": r[0],
            "product_norm": r[1],
            "price": r[2],
            "unit_norm": r[3],
            "meta": json.loads(r[4] or "{}"),
        } for r in rows
    ]
