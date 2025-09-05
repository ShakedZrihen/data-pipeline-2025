from fastapi import APIRouter , HTTPException
from typing import List
from db.db import get_conn
from models.models import StoreResponse

router = APIRouter(prefix="/stores", tags=["stores"])

@router.get("", response_model=List[StoreResponse])
def list_stores():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT store_id, chain_id, store_name, address, city FROM stores ORDER BY store_name""")
        rows = cur.fetchall()
    return rows

