from fastapi import APIRouter, HTTPException
from typing import List
from db.db import get_conn
from models.models import SupermarketResponse, StoreResponse, ProductResponse

router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])

@router.get("", response_model=List[SupermarketResponse])
def list_supermarkets():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT chain_id, chain_name FROM supermarkets""")
        rows = cur.fetchall()
    return [{"chain_id": r["chain_id"], "name": r["chain_name"]} for r in rows]

@router.get("/{chain_id}", response_model=SupermarketResponse)
def get_supermarket(chain_id: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT chain_id, chain_name FROM supermarkets WHERE chain_id = %s""", (chain_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Supermarket not found")
    return {"chain_id": row["chain_id"], "name": row["chain_name"]}

@router.get("/{chain_id}/stores", response_model=List[StoreResponse])
def list_stores_for_supermarket(chain_id: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT store_id, chain_id, store_name, address, city FROM stores WHERE chain_id = %s ORDER BY store_name""", (chain_id,))
        rows = cur.fetchall()
    return rows

