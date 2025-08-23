import os
from datetime import date
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, HTTPException, Query
from supabase import create_client, Client
from dotenv import load_dotenv
from pathlib import Path

# ========= Load .env early (cross-platform) =========
# 1) Try project-root .env via cwd (useful when running uvicorn from repo root)
loaded = load_dotenv()

# 2) Fallback: explicit relative path to consumer/.env (as you suggested)
if not loaded:
    consumer_env = Path(__file__).resolve().parent.parent.parent / "consumer" / ".env"
    load_dotenv(consumer_env.as_posix())

# ========= Supabase Client (reads from env) =========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    # Helpful error so זה ברור איפה מחפשים את ה-.env
    raise RuntimeError(
        "Missing SUPABASE_URL / SUPABASE_KEY. "
        "Tried default .env and consumer/.env"
    )

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Create API Router
router = APIRouter()

# ===================== Helpers =====================

def _parse_float(x: Any, default: float = 0.0) -> float:
    """Robust float parser for numeric strings like '34.200' or actual numbers."""
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        return float(str(x).replace(",", "."))
    except Exception:
        return default

def _today_str() -> str:
    """Return today's date as YYYY-MM-DD (used for promotion validity checks)."""
    return date.today().isoformat()

def _shape_product_row(price_row: Dict[str, Any], promo_row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Project only the required fields for the response."""
    return {
        "item_code": price_row.get("item_code"),       # barcode
        "item_name": price_row.get("item_name"),       # product name
        "store_id": price_row.get("store_id"),         # branch
        "chain_id": price_row.get("chain_id"),         # chain
        "has_promotion": promo_row is not None,        # promotion exists
        "discount_rate": _parse_float((promo_row or {}).get("discount_rate"), 0.0),
        "price": _parse_float(price_row.get("qty_price"), 0.0),  # current price
        "store_address": price_row.get("store_address"),
        "store_city": price_row.get("store_city"),      # store city
        "company_name": price_row.get("company_name") # company name
    }

def _fetch_active_promotion_for_item(item_code: str, chain_id: str, store_id: str) -> Optional[Dict[str, Any]]:
    """
    Return an active promotion for (item_code, chain_id, store_id) if exists.
    Active = additional_is_active == true AND today's date within start/end dates.
    If multiple exist, returns the first (can be refined by recency if needed).
    """
    today = _today_str()

    resp = (
        supabase.table("promotions")
        .select(
            "promotion_id,promotion_description,discount_rate,reward_type,"
            "promotion_start_date,promotion_end_date,additional_is_active,"
            "item_code,chain_id,store_id"
        )
        .eq("item_code", item_code)
        .eq("chain_id", chain_id)
        .eq("store_id", store_id)
        .eq("additional_is_active", True)
        .execute()
    )
    promos = resp.data or []

    active = []
    for pr in promos:
        start_ok = (pr.get("promotion_start_date") or "") <= today
        end_ok = today <= (pr.get("promotion_end_date") or "")
        if start_ok and end_ok:
            active.append(pr)

    if not active:
        return None
    return active[0]

# ===================== PROMOTIONS (example) =====================

@router.get("/promotions", summary="Get a sample of promotions (debug)")
async def get_promotions():
    """Return a small sample of promotions for debugging."""
    try:
        data = supabase.table("promotions").select("*").limit(25).execute().data
        return {"promotions": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ===================== PRODUCTS / PRICES =====================

@router.get("/product/by-barcode/{item_code}", summary="Get product(s) by barcode")
async def get_product_by_barcode(item_code: str):
    """
    Returns all store occurrences of a product by barcode, including price,
    whether an active promotion exists, and the discount rate if applicable.
    """
    try:
        price_rows = (
            supabase.table("prices")
            .select("item_code,item_name,qty_price,chain_id,company_name,store_id,store_city,store_address")
            .eq("item_code", item_code)
            .execute()
        ).data or []

        if not price_rows:
            raise HTTPException(status_code=404, detail="Item not found")

        results: List[Dict[str, Any]] = []
        for row in price_rows:
            promo = _fetch_active_promotion_for_item(
                item_code=row["item_code"],
                chain_id=row["chain_id"],
                store_id=row["store_id"],
            )
            results.append(_shape_product_row(row, promo))

        return {"products": results}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/product/by-name", summary="Search products by name (case-insensitive)")
async def get_product_by_name(q: str = Query(..., description="Substring to search within item_name")):
    """
    Case-insensitive search on item_name (ILIKE %q%).
    Returns all matching store occurrences including price and promotion info.
    """
    try:
        price_rows = (
            supabase.table("prices")
            .select("item_code,item_name,qty_price,chain_id,company_name,store_id,store_city,store_address")
            .ilike("item_name", f"%{q}%")
            .execute()
        ).data or []

        if not price_rows:
            raise HTTPException(status_code=404, detail="No products match this name")

        results: List[Dict[str, Any]] = []
        for row in price_rows:
            promo = _fetch_active_promotion_for_item(
                item_code=row["item_code"],
                chain_id=row["chain_id"],
                store_id=row["store_id"],
            )
            results.append(_shape_product_row(row, promo))

        return {"products": results}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ===================== STORES =====================

@router.get("/stores", summary="Get stores (optionally filter by chain_id)")
async def get_stores(chain_id: Optional[str] = Query(None, description="Chain to filter by (optional)")):
    """
    Returns unique stores from the prices table. If chain_id is provided,
    only stores belonging to that chain are returned.
    """
    try:
        query = supabase.table("prices").select("store_id,chain_id,store_address,store_city")
        if chain_id:
            query = query.eq("chain_id", chain_id)
        rows = query.execute().data or []

        seen = set()
        deduped = []
        for r in rows:
            key = (r.get("store_id"), r.get("chain_id"), r.get("store_address"), r.get("store_city"))
            if key not in seen:
                seen.add(key)
                deduped.append(
                    {
                        "store_id": key[0], 
                        "chain_id": key[1], 
                        "store_address": key[2],
                        "store_city": key[3]
                    }
                )

        return {"stores": deduped}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
