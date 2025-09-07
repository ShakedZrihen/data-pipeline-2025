from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
import zlib
from typing import List, Optional
from ..database import get_db
from ..schemas import ProductResponse, PriceComparisonResponse

router = APIRouter(prefix="/products", tags=["products"])

@router.get("", response_model=List[ProductResponse])
def search_products(
    name: str | None = None,
    promo: bool | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    supermarket_id: int | None = None,
    limit: int = 200,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    sql = """
    WITH lp AS (
      SELECT
        p.provider,
        p.branch_code,
        p.product_code,
        p.product_name,
        p.unit,
        p.price,
        p.ts,
        ROW_NUMBER() OVER (
          PARTITION BY p.provider, p.branch_code, p.product_code
          ORDER BY p.ts DESC
        ) AS rn
      FROM price_item p
    )
    SELECT
      s.supermarket_id        AS supermarket_id,
      s.name                  AS supermarket_name,
      lp.provider,
      lp.branch_code,
      lp.product_code         AS barcode,
      lp.product_name         AS canonical_name,
      NULL::text              AS brand,
      NULL::text              AS category,
      NULL::numeric           AS size_value,
      lp.unit                 AS size_unit,
      lp.price::float         AS price,
      'ILS'::text             AS currency,
      promo.price             AS promo_price,
      promo.description       AS promo_text,
      TRUE                    AS in_stock,
      lp.ts                   AS collected_at
    FROM lp
    JOIN supermarket s
      ON s.provider = lp.provider
     AND s.branch_code = lp.branch_code
    LEFT JOIN LATERAL (
      SELECT pr.price::float AS price, pr.description
      FROM promo_item pr
      WHERE pr.provider = lp.provider
        AND pr.branch_code = lp.branch_code
        AND pr.product_code = lp.product_code
        AND (pr.start_ts IS NULL OR pr.start_ts <= now())
        AND (pr.end_ts   IS NULL OR pr.end_ts   >= now())
      ORDER BY pr.start_ts DESC NULLS LAST
      LIMIT 1
    ) AS promo ON TRUE
    WHERE lp.rn = 1
      AND (:q   IS NULL OR lp.product_name ILIKE '%' || :q   || '%')
      AND (:sid IS NULL OR s.supermarket_id = :sid)
      AND (:pmin IS NULL OR lp.price >= :pmin)
      AND (:pmax IS NULL OR lp.price <= :pmax)
      AND (
        :promo_flag IS NULL
        OR (:promo_flag = TRUE  AND promo.price IS NOT NULL)
        OR (:promo_flag = FALSE AND promo.price IS NULL)
      )
    ORDER BY lp.product_name
    LIMIT :limit OFFSET :offset
    """
    params = {
        "q": name,
        "sid": supermarket_id,
        "pmin": min_price,
        "pmax": max_price,
        "promo_flag": promo,
        "limit": limit,
        "offset": offset,
    }
    rows = db.execute(text(sql), params).mappings().all()

    out = []
    for r in rows:
        pid = zlib.crc32(f"{r['provider']}#{r['branch_code']}#{r['barcode']}".encode()) & 0xFFFFFFFF
        out.append({
            "product_id": pid,
            "supermarket_id": r["supermarket_id"],
            "barcode": r["barcode"],
            "canonical_name": r["canonical_name"],
            "brand": None,
            "category": None,
            "size_value": None,
            "size_unit": r["size_unit"],
            "price": r["price"],
            "currency": r["currency"],
            "promo_price": r["promo_price"],
            "promo_text": r["promo_text"],
            "in_stock": r["in_stock"],
            "collected_at": r["collected_at"].isoformat() if r["collected_at"] else None,
        })
    return out

@router.get("/barcode/{barcode}", response_model=List[PriceComparisonResponse])
def by_barcode(barcode: str, db: Session = Depends(get_db)):
    sql = """
    WITH lp AS (
      SELECT
        p.provider,
        p.branch_code,
        p.product_code,
        p.product_name,
        p.unit,
        p.price,
        p.ts,
        ROW_NUMBER() OVER (
          PARTITION BY p.provider, p.branch_code, p.product_code
          ORDER BY p.ts DESC
        ) AS rn
      FROM price_item p
      WHERE p.product_code = :code
    )
    SELECT
      s.supermarket_id        AS supermarket_id,
      s.name                  AS supermarket_name,
      lp.provider,
      lp.branch_code,
      lp.product_code         AS barcode,
      lp.product_name         AS canonical_name,
      lp.unit                 AS size_unit,
      lp.price::float         AS price,
      promo.price             AS promo_price,
      promo.description       AS promo_text
    FROM lp
    JOIN supermarket s
      ON s.provider = lp.provider
     AND s.branch_code = lp.branch_code
    LEFT JOIN LATERAL (
      SELECT pr.price::float AS price, pr.description
      FROM promo_item pr
      WHERE pr.provider = lp.provider
        AND pr.branch_code = lp.branch_code
        AND pr.product_code = lp.product_code
        AND (pr.start_ts IS NULL OR pr.start_ts <= now())
        AND (pr.end_ts   IS NULL OR pr.end_ts   >= now())
      ORDER BY pr.start_ts DESC NULLS LAST
      LIMIT 1
    ) AS promo ON TRUE
    WHERE lp.rn = 1
    ORDER BY COALESCE(promo.price, lp.price) ASC
    """
    rows = db.execute(text(sql), {"code": barcode}).mappings().all()
    if not rows:
        return []

    best = min((r["promo_price"] if r["promo_price"] is not None else r["price"]) for r in rows)
    out = []
    for r in rows:
        eff = r["promo_price"] if r["promo_price"] is not None else r["price"]
        savings = round(eff - best, 2)
        pid = zlib.crc32(f"{r['provider']}#{r['branch_code']}#{r['barcode']}".encode()) & 0xFFFFFFFF
        out.append({
            "product_id": pid,
            "supermarket_id": r["supermarket_id"],
            "supermarket_name": r["supermarket_name"],
            "canonical_name": r["canonical_name"],
            "brand": None,
            "category": None,
            "barcode": r["barcode"],
            "price": r["price"],
            "promo_price": r["promo_price"],
            "promo_text": r["promo_text"],
            "size_value": None,
            "size_unit": r["size_unit"],
            "in_stock": True,
            "savings": savings
        })
    return out