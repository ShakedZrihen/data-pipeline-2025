
from datetime import datetime

from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List
from ..database import conn


router = APIRouter(prefix="/products", tags=["products"])
cur = conn.cursor()

class Product(BaseModel):
    id: int
    supermarket_id: int
    barcode: str
    name: str
    brand: Optional[str]
    category: Optional[str] = None
    unit_of_measure: Optional[str]
    quantity: Optional[float]
    price: float
    promo_price: Optional[float]
    promo: bool
    city: Optional[str]
    address: Optional[str]
    currency: str = "ILS"
    created_at: datetime


@router.get("", response_model=List[Product])
def list_products(name: Optional[str] = None, 
    promo: Optional[bool] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    supermarket_id: Optional[int] = None):

    query = """
    SELECT
        pp.id,
        pp.supermarket_id,
        pp.barcode,
        pp.name,
        pp.brand,
        NULL AS category,
        pp.unit_of_measure,
        pp.quantity,
        cp_reg.price AS price,
        cp_promo.price AS promo_price,
        (cp_promo.price IS NOT NULL) AS promo,
        b.city,
        b.address,
        'ILS' AS currency,
        pp.created_at
    FROM provider_products pp
    JOIN current_prices cp_reg
        ON pp.id = cp_reg.provider_product_id AND cp_reg.price_type = 'regular'
    LEFT JOIN current_prices cp_promo
        ON pp.id = cp_promo.provider_product_id AND cp_promo.price_type = 'promo'
    JOIN branches b
        ON cp_reg.branch_id = b.id
    WHERE 1=1
    """
    params = []
    if name:
        query += " AND pp.name ILIKE %s"
        params.append(f"%{name}%")

    if supermarket_id is not None:
        query += " AND pp.supermarket_id = %s"
        params.append(supermarket_id)

    if min_price is not None:
        query += " AND cp_reg.price >= %s"
        params.append(min_price)

    if max_price is not None:
        query += " AND cp_reg.price <= %s"
        params.append(max_price)

    if promo is True:
        query += " AND cp_promo.price IS NOT NULL"
    elif promo is False:
        query += " AND cp_promo.price IS NULL"

    query += " ORDER BY pp.name LIMIT 100"

    cur.execute(query, params)
    rows = cur.fetchall()

    return rows


@router.get("/barcode/{barcode}", response_model=List[Product])
def get_products_by_barcode(
    barcode: str ):
    
    query = """
    SELECT
        pp.id,
        pp.supermarket_id,
        pp.barcode,
        pp.name,
        pp.brand,
        NULL AS category,
        pp.unit_of_measure,
        pp.quantity,
        cp_reg.price AS price,
        cp_promo.price AS promo_price,
        (cp_promo.price IS NOT NULL) AS promo,
        b.city,
        b.address,
        'ILS' AS currency,
        pp.created_at
    FROM provider_products pp
    JOIN current_prices cp_reg
        ON pp.id = cp_reg.provider_product_id AND cp_reg.price_type = 'regular'
    LEFT JOIN current_prices cp_promo
        ON pp.id = cp_promo.provider_product_id AND cp_promo.price_type = 'promo'
    JOIN branches b
        ON cp_reg.branch_id = b.id
    WHERE pp.barcode = %s
    ORDER BY cp_reg.price ASC
    """

    cur.execute(query, [barcode])
    rows = cur.fetchall()

    return rows
