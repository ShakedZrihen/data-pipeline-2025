
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List
from ..database import _conn

router = APIRouter(prefix="/products", tags=["products"])

class Product(BaseModel):
    id: int
    supermarket_id: int
    barcode: Optional[str]
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
    created_at: Optional[datetime] = None


@router.get("/", response_model=List[Product])
def get_products(
    name: Optional[str] = Query(None, description="Filter by product name"),
    promo: Optional[bool] = Query(None, description="Filter by promotion status"),
    min_price: Optional[float] = Query(None),
    max_price: Optional[float] = Query(None),
    supermarket_id: Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    sql = """
    WITH reg AS (
      SELECT provider_product_id, branch_id, price AS reg_price
      FROM public.current_prices
      WHERE price_type = 'regular'
    ),
    pro AS (
      SELECT provider_product_id, branch_id, price AS promo_price
      FROM public.current_prices
      WHERE price_type = 'promo'
    ),
    per_branch AS (
      SELECT
        COALESCE(r.provider_product_id, p.provider_product_id) AS provider_product_id,
        COALESCE(r.branch_id,         p.branch_id)            AS branch_id,
        r.reg_price,
        p.promo_price
      FROM reg r
      FULL JOIN pro p
        ON r.provider_product_id = p.provider_product_id
       AND r.branch_id          = p.branch_id
      WHERE r.reg_price IS NOT NULL OR p.promo_price IS NOT NULL
    )
    SELECT
      pr.id                                           AS id,
      pr.supermarket_id                               AS supermarket_id,
      pr.barcode                                      AS barcode,
      pr.name                                         AS name,
      pr.brand                                        AS brand,
      NULL::text                                      AS category,
      pr.unit_of_measure                              AS unit_of_measure,
      pr.quantity::float8                             AS quantity,
      COALESCE(pb.promo_price, pb.reg_price)::float8  AS price,        -- prefer promo
      pb.promo_price::float8                          AS promo_price,
      (pb.promo_price IS NOT NULL)                    AS promo,        -- promo exists
      b.city                                          AS city,
      b.address                                       AS address,
      'ILS'::text                                     AS currency,
      pr.created_at                                   AS created_at
    FROM public.provider_products pr
    JOIN per_branch pb
      ON pb.provider_product_id = pr.id
    JOIN public.branches b
      ON b.id = pb.branch_id
    WHERE
      (%(supermarket_id)s IS NULL OR pr.supermarket_id = %(supermarket_id)s)
      AND (%(name)s IS NULL
           OR pr.name ILIKE %(name_like)s
           OR pr.barcode = %(name)s) -- optional: treat exact barcode typed into 'name' as match
      AND (%(promo)s IS NULL
           OR (%(promo)s = TRUE  AND pb.promo_price IS NOT NULL)
           OR (%(promo)s = FALSE AND pb.promo_price IS NULL))
      AND (%(min_price)s IS NULL OR COALESCE(pb.promo_price, pb.reg_price) >= %(min_price)s)
      AND (%(max_price)s IS NULL OR COALESCE(pb.promo_price, pb.reg_price) <= %(max_price)s)
    ORDER BY COALESCE(pb.promo_price, pb.reg_price) ASC,
             pr.supermarket_id,
             pr.created_at DESC,
             pr.id DESC
    LIMIT %(limit)s OFFSET %(offset)s;
    """

    params = {
        "name": name,
        "name_like": f"%{name}%" if name else None,
        "promo": promo,
        "min_price": min_price,
        "max_price": max_price,
        "supermarket_id": supermarket_id,
        "limit": limit,
        "offset": offset,
    }

    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


@router.get("/barcode/{barcode}", response_model=List[Product])
def compare_by_barcode(
    barcode: str,
    limit: Optional[int] = Query(100, ge=1, le=500),
    offset: Optional[int] = Query(0, ge=0)
):
    sql = """
    WITH products AS (
      SELECT *
      FROM public.provider_products
      WHERE barcode = %(barcode)s
    ),
    reg AS (
      SELECT provider_product_id, branch_id, price AS reg_price
      FROM public.current_prices
      WHERE price_type = 'regular'
    ),
    pro AS (
      SELECT provider_product_id, branch_id, price AS promo_price
      FROM public.current_prices
      WHERE price_type = 'promo'
    ),
    per_branch AS (
      SELECT
        COALESCE(r.provider_product_id, p.provider_product_id) AS provider_product_id,
        COALESCE(r.branch_id,         p.branch_id)            AS branch_id,
        r.reg_price,
        p.promo_price
      FROM reg r
      FULL JOIN pro p
        ON r.provider_product_id = p.provider_product_id
       AND r.branch_id          = p.branch_id
      WHERE r.reg_price IS NOT NULL OR p.promo_price IS NOT NULL
    )
    SELECT
      pr.id                                        AS id,
      pr.supermarket_id                            AS supermarket_id,
      pr.barcode                                   AS barcode,
      pr.name                                      AS name,
      pr.brand                                     AS brand,
      NULL::text                                   AS category,
      pr.unit_of_measure                           AS unit_of_measure,
      pr.quantity::float8                          AS quantity,
      COALESCE(pb.promo_price, pb.reg_price)::float8 AS price,
      pb.promo_price::float8                       AS promo_price,
      (pb.promo_price IS NOT NULL)                 AS promo,
      b.city                                       AS city,
      b.address                                    AS address,
      'ILS'::text                                  AS currency,
      pr.created_at                                AS created_at
    FROM products pr
    JOIN per_branch pb
      ON pb.provider_product_id = pr.id
    JOIN public.branches b
      ON b.id = pb.branch_id
    ORDER BY COALESCE(pb.promo_price, pb.reg_price) ASC,
             pr.supermarket_id,
             pr.created_at DESC,
             pr.id DESC
    LIMIT %(limit)s OFFSET %(offset)s;
    """
    params = {"barcode": barcode, "limit": limit, "offset": offset}

    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


