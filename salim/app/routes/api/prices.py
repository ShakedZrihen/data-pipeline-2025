from fastapi import APIRouter, Depends, Query, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from ...db import get_session
from ...utils.stores import enrich_row, resolve_provider_id_or_404

router = APIRouter(prefix="/prices", tags=["prices"])

@router.get("/list_prices", summary="return the first 10 rows of prices table, combined with the name of the provider & branch")
async def list_prices(session: AsyncSession = Depends(get_session)):
    sql = text("""
        SELECT provider, branch, ts, product_id, product, price, unit, brand, item_type
        FROM prices_full
        LIMIT 10
    """)
    rows = (await session.execute(sql)).mappings().all()
    items = [enrich_row(dict(r)) for r in rows]
    return {"items": items}


@router.get("/by-barcode", summary="Compare prices by barcode. (max 100 results)")
async def get_prices_by_barcode(
    product_id: str = Query(..., description="example for a barocde that has many different prices: 10900678148"),
    session: AsyncSession = Depends(get_session)
):
    sql = text("""
        SELECT provider, branch, product_id, product, price, unit, brand
        FROM prices_full
        WHERE product_id = :product_id
        ORDER BY price ASC
        LIMIT 100
    """)
    rows = (await session.execute(sql, {"product_id": product_id})).mappings().all()
    items = [enrich_row(dict(r)) for r in rows]
    return {"items": items}


@router.get("/products", summary="Search products with filters, ordered by price. (max 100 results)")
async def search_products(
    name: str = Query(None, description="Filter by product name"),
    min_price: float = Query(None, description="Minimum price"),
    max_price: float = Query(None, description="Maximum price"),
    provider: str = Query(None, description="provider name or id, for example: '7290055700007' or 'קרפור'"),
    session: AsyncSession = Depends(get_session)
):
    query = """
        SELECT provider, branch, ts, product_id, product, price, unit, brand, item_type
        FROM prices_full
        WHERE 1=1
    """

    params = {}

    if name:
        query += " AND product ILIKE :name"
        params["name"] = f"%{name}%"

    if min_price is not None:
        query += " AND price >= :min_price"
        params["min_price"] = min_price

    if max_price is not None:
        query += " AND price <= :max_price"
        params["max_price"] = max_price

    if provider:
        provider_id = resolve_provider_id_or_404(provider)
        query += " AND provider = :provider"
        params["provider"] = provider_id

    query += " ORDER BY price ASC LIMIT 100"

    rows = (await session.execute(text(query), params)).mappings().all()
    items = [enrich_row(dict(r)) for r in rows]
    return {"items": items}


@router.get(
    "/{provider}/products",
    summary="Get products from a specific supermarket (max 100 results)",
    description="Returns a list of products sold by the given supermarket (by ID or name). Supports optional search by product name."
)
async def get_products_by_supermarket(
    provider: str = Path(..., description="Supermarket name or ID (e.g. 7290103152017 or 'אושר עד')"),
    search: str = Query(None, description="Search term for product name. for example: 'עד חצות'"),
    session: AsyncSession = Depends(get_session)
):

    provider_id = resolve_provider_id_or_404(provider)

    query = """
        SELECT provider, branch, ts, product_id, product, price, unit, brand, item_type
        FROM prices_full
        WHERE provider = :provider
    """
    params = {"provider": provider_id}

    if search:
        query += " AND product ILIKE :search"
        params["search"] = f"%{search}%"

    query += " ORDER BY price ASC LIMIT 100"

    rows = (await session.execute(text(query), params)).mappings().all()
    items = [enrich_row(dict(r)) for r in rows]
    return {"items": items}

