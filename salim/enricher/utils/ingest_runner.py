from __future__ import annotations
from typing import Any, Dict, List

from .db import (
    ensure_schema, get_conn,
    upsert_supermarket, upsert_store,
    fetch_existing_items, batch_upsert_items, clear_expired_promos
)
from .merge_logic import (
    build_rows_from_prices,
    build_rows_from_promos,
)

ensure_schema() 

def _chain_id(provider: str | None) -> str:
    return (provider or "").strip().lower()

def _store_id(payload: Dict[str, Any]) -> str:
    # prefer payload.store_id (if extractor put it) else branch
    return str(payload.get("store_id") or payload.get("branch") or "").strip()

def ingest_payload_to_db(payload: Dict[str, Any]) -> None:
    """
    Accepts one normalized payload (pricesFull or promoFull) and upserts into items_current
    using merge rules from merge_logic.py (Python-side, not SQL).
    """
    chain_id = _chain_id(payload.get("provider"))
    store_id = _store_id(payload)
    ptype    = payload.get("type")
    items    = payload.get("items") or []
    ts       = payload.get("timestamp") or None

    # bootstrap chain & store (name defaults are fine for now)
    with get_conn() as conn:
        upsert_supermarket(conn, chain_id, chain_name=payload.get("provider") or chain_id or "unknown")
        upsert_store(conn, store_id, store_name=str(store_id))
        conn.commit()

    # fetch current rows for only the relevant codes
    codes = [str(i.get("code")) for i in items if i.get("code")]
    with get_conn() as conn:
        existing = fetch_existing_items(conn, store_id, codes)

    if ptype == "pricesFull":
        rows = build_rows_from_prices(chain_id, store_id, ts, items, existing)
        with get_conn() as conn:
            batch_upsert_items(conn, rows)
            conn.commit()
    elif ptype == "promoFull":
        rows = build_rows_from_promos(chain_id, store_id, ts, items, existing)
        with get_conn() as conn:
            batch_upsert_items(conn, rows)
            clear_expired_promos(conn)
            conn.commit()
    else:
        # treat unknown as prices
        rows = build_rows_from_prices(chain_id, store_id, ts, items, existing)
        with get_conn() as conn:
            batch_upsert_items(conn, rows)
            conn.commit()
