from __future__ import annotations
from typing import Any, Dict, List

from .db import (
    ensure_schema, get_conn,
    upsert_supermarket, upsert_store,
    fetch_existing_items, batch_upsert_items, clear_expired_promos , fetch_chain_id
)
from .merge_logic import (
    build_rows_from_prices,
    build_rows_from_promos,
)

ensure_schema() 



def _store_id(payload: Dict[str, Any]) -> str:
    return str(payload.get("store_id") or payload.get("branch") or "").strip()

def ingest_payload_to_db(payload: Dict[str, Any]) -> None:
    provider = (payload.get("provider") or "").lower()
    with get_conn() as conn:
        chain_id = fetch_chain_id(conn, provider)
    store_id = _store_id(payload)
    ptype    = payload.get("type")
    items    = payload.get("items") or []
    ts       = payload.get("timestamp") or None
    print(f"Ingesting payload for chain_id={chain_id}, store_id={store_id}, type={ptype}, items={len(items)}")

    # try:
    #     upsert_supermarket(get_conn(), chain_id, chain_name=payload.get("provider") or chain_id or "unknown")
    #     conn.commit()
    # except Exception as e:
    #     print(f"Failed to upsert supermarket: {e}")

    # try:
    #     upsert_store(get_conn(), chain_id, store_id, store_name=str(store_id))
    #     conn.commit()
    # except Exception as e:
    #     print(f"Failed to upsert store: {e}")

    codes = [str(i.get("code")) for i in items if i.get("code")]
    with get_conn() as conn:
        existing = fetch_existing_items(conn, chain_id, store_id, codes)

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
