from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List
from .products import Product
from ..database import conn



router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])

cur = conn.cursor()

class Supermarket(BaseModel):
    id: int
    name: str
    created_at: datetime


@router.get("", response_model=list[Supermarket])
def list_supermarkets():
    Query = """SELECT * FROM supermarkets;"""
    cur.execute(Query)
    rows = cur.fetchall()

    return rows


@router.get("/{supermarket_id}", response_model=Supermarket, tags=["supermarkets"])
def get_supermarket(supermarket_id: int):
    query = "SELECT * FROM supermarkets WHERE id = %s"
    cur.execute(query, (supermarket_id,))
    row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Supermarket not found")
    return row


@router.get("/{supermarket_id}/products", response_model=List[Product], tags=["supermarkets"])
def get_products_for_supermarket(
    supermarket_id: int,
    search: Optional[str] = Query(None, description="Filter by product name")
):
    if search:
        query = """
            SELECT * FROM provider_products
            WHERE supermarket_id = %s AND name ILIKE %s
        """
        cur.execute(query, (supermarket_id, f"%{search}%"))
    else:
        query = "SELECT * FROM provider_products WHERE supermarket_id = %s"
        cur.execute(query, (supermarket_id,))
    
    rows = cur.fetchall()
    return rows