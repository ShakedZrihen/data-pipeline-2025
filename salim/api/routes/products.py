from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..database import conn
from .supermarket import to_json

router = APIRouter(prefix="/products", tags=["products"])

cur = conn.cursor()


@router.get("/")
async def get_products(
    name: Optional[str] = Query(
        None, Desccription="Name or part of the name of a product"
    ),
    min_price: Optional[float] = Query(
        None,
        Desccription="""Minimum price to search for. if the min_price param is used, you also have to
        use the max_price param.
        """,
    ),
    max_price: Optional[float] = Query(
        None,
        Desccription="""Maximum price to search for. if the max_price param is used, you also have to
        use the min_price param.
        """,
    ),
):
    if (max_price is None and min_price is not None) or (
        min_price is None and max_price is not None
    ):
        raise HTTPException(
            status_code=400,
            detail="price filter must include both min_price and max_price.",
        )

    search_term = f"%{name}%"
    if name and max_price:
        query = """
        SELECT *
        FROM pricing
        WHERE product_name ILIKE %s
        AND price BETWEEN %s AND %s;
        """
        params = (search_term, min_price, max_price)

    elif name and not max_price:
        query = """
        SELECT *
        FROM pricing
        WHERE product_name ILIKE %s;
        """
        params = (search_term,)
    elif max_price and not name:
        query = """
        SELECT *
        FROM pricing
        WHERE price BETWEEN %s AND %s;
        """
        params = (min_price, max_price)
    else:
        query = """ SELECT * FROM pricing; """
        params = ()

    cur.execute(query, params)
    products = cur.fetchall()
    # Convert rows to a list of dictionaries
    records = to_json(cur, products)
    return records
