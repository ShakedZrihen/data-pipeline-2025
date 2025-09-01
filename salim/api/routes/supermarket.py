from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Path, Query
from pydantic import BaseModel

from ..database import conn

router = APIRouter(prefix="/supermarkets", tags=["supermarkets"])

cur = conn.cursor()


def to_json(cur, lst):
    records = []
    for row in lst:
        record = {}
        for i, column in enumerate(cur.description):
            record[column.name] = row[i]
        records.append(record)
    return records


class SupermarketResponse(BaseModel):
    _id: str
    supermarkets: str
    id: int
    created_at: Optional[datetime]
    website: Optional[str]
    city: Optional[str]
    branch: Optional[str]


@router.get("/")
async def get_supermarkets() -> list[SupermarketResponse]:
    """
    Get a list of all the supermarkets.
    """
    query = "SELECT * FROM supermarkets"
    cur.execute(query)
    supermarkets = cur.fetchall()
    # Convert rows to a list of dictionaries
    records = to_json(cur, supermarkets)
    return records


@router.get("/{supermarket_id}")
async def get_super_by_id(
    supermarket_id: int = Path(..., description="The internal ID of the supermarket"),
):
    """
    Get supermarket info by id.
    """
    query = "SELECT * FROM supermarkets WHERE id = %s"
    cur.execute(query, (id,))
    supermarket = cur.fetchone()
    if supermarket is None:
        return {"error": "Supermarket not found"}
    # Convert row to a dictionary
    record = {}
    for i, column in enumerate(cur.description):
        record[column.name] = supermarket[i]
    return record


@router.get("/{supermarkets_id}/products")
async def get_super_products(
    supermarkets_id: int = Path(..., description="The supermarket's internal ID"),
    search: Optional[str] = Query(
        None, description="Optional search term for product names"
    ),
):
    """
    Get a specific supermarket products.
    """
    if search:
        query = """
        SELECT p.*
        FROM pricing AS p
        JOIN supermarkets AS s
        ON p.chain_name = s.supermarkets
        WHERE s.id = %s
        AND p.product_name ILIKE %s;
        """
        search_term = f"%{search}%"
        params = (supermarkets_id, search_term)
    else:
        query = """
        SELECT p.*
        FROM pricing AS p
        JOIN supermarkets AS s
        ON p.chain_name = s.supermarkets
        WHERE s.id = %s;
        """
        params = (supermarkets_id,)

    cur.execute(query, params)
    products = cur.fetchall()
    # Convert rows to a list of dictionaries
    records = to_json(cur, products)
    return records
