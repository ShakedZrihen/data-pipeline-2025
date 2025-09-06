from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from ...db import get_session
from ...utils.stores import enrich_row, enrich_provider_branch, resolve_provider_id_or_404

router = APIRouter(prefix="/promos", tags=["promos"])

@router.get("/list_promos", summary="return the first 10 rows of promos table, combined with the name of the provider & branch")
async def list_prices(session: AsyncSession = Depends(get_session)):
    sql = text("""
        SELECT
          id,
          provider,
          branch,
          ts,
          product_id,
          promo_desc,
          discounted_price::float AS discounted_price,
          COALESCE(min_qty, 1)::int AS min_qty,
          brand,
          item_type
        FROM promos_full
        LIMIT 10
    """)
    rows = (await session.execute(sql)).mappings().all()
    items = [enrich_row(dict(r)) for r in rows]
    return {"items": items}


@router.get("/by-barcode", summary="Get grouped promotions by barcode", description="return the promotion of a certain barcode + list of barcodes of the same promotion. barcode for example: '7290002007357'")
async def get_grouped_promos_by_barcode(
    product_id: str = Query(..., description="Product barcode"),
    session: AsyncSession = Depends(get_session)
):
    sql = text("""
        WITH matching_promos AS (
            SELECT *
            FROM promos_full
            WHERE product_id = :product_id
        )
        SELECT
            p.provider,
            p.branch,
            p.ts,
            p.promo_desc,
            ARRAY_AGG(DISTINCT p.product_id) AS product_ids
        FROM promos_full p
        JOIN matching_promos mp
          ON p.provider = mp.provider
         AND p.branch = mp.branch
         AND p.ts = mp.ts
         AND p.promo_desc = mp.promo_desc
        GROUP BY p.provider, p.branch, p.ts, p.promo_desc
        ORDER BY p.ts DESC
    """)
    rows = (await session.execute(sql, {"product_id": product_id})).mappings().all()

    # העשרה של השמות לפי קבצי JSON
    items = [enrich_provider_branch(dict(r)) for r in rows]

    return {"items": items}


@router.get("/search", summary="Search promo and group products by provider/branch/promo", description="example- provider:'7290873255550', branch: '002', desc: 'שוקולד'")
async def search_promo_grouped(
    provider: str = Query(..., description="Provider name or ID"),
    branch: str = Query(..., description="Branch code"),
    desc: str = Query(..., description="Part of the promo description"),
    session: AsyncSession = Depends(get_session)
):
    
    provider_id = resolve_provider_id_or_404(provider)

    sql = text("""
        SELECT
          provider,
          branch,
          ts,
          promo_desc,
          MIN(discounted_price) AS discounted_price,
          MIN(min_qty) AS min_qty,
          ARRAY_AGG(DISTINCT product_id) AS product_ids
        FROM promos_full
        WHERE provider = :provider
          AND branch = :branch
          AND promo_desc ILIKE :desc
        GROUP BY provider, branch, ts, promo_desc
        ORDER BY ts DESC
    """)

    rows = (await session.execute(sql, {
        "provider": provider_id,
        "branch": branch,
        "desc": f"%{desc}%"
    })).mappings().all()

    return {"items": [enrich_provider_branch(dict(r)) for r in rows]}
