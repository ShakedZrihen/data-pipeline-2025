
import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

def _to_bool(val: Any) -> Optional[bool]:
    if val is None:
        return None
    s = str(val).strip()
    if s in {"1", "true", "True", "TRUE"}:
        return True
    if s in {"0", "false", "False", "FALSE"}:
        return False
    return None

def _to_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        s = str(val).replace(",", "").strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None

def _to_datetime(val: Any) -> Optional[str]:
    if not val:
        return None
    s = str(val).strip()
    # Accept formats like '2025-01-01 09:49:30' or ISO strings
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    # give up but return original string
    return s

def enrich_items_from_extractor_json(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Accepts a single extractor JSON file structure:
    {
      "original_filename": "...",
      "extraction_timestamp": "...",
      "file_size": int,
      "last_modified": "...",
      "data": { XmlDocVersion, ChainId, SubChainId, StoreId, Items: { Item: [...] } }
    }
    Returns a list of normalized price rows.
    """
    meta = {
        "source_file": doc.get("original_filename"),
        "extraction_timestamp": doc.get("extraction_timestamp"),
        "last_modified": doc.get("last_modified"),
    }

    data = doc.get("data") or {}
    chain_id = data.get("ChainId")
    sub_chain_id = data.get("SubChainId")
    store_id = data.get("StoreId")
    items = (((data or {}).get("Items") or {}).get("Item")) or []

    # Some feeds provide a single dict instead of list
    if isinstance(items, dict):
        items = [items]

    rows: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        row = {
            "chain_id": chain_id,
            "sub_chain_id": sub_chain_id,
            "store_id": store_id,
            "item_code": (it.get("ItemCode") or "").strip() or None,
            "item_name": (it.get("ItemName") or "").strip() or None,
            "manufacturer_name": (it.get("ManufacturerName") or "").strip() or None,
            "manufacture_country": (it.get("ManufactureCountry") or "").strip() or None,
            "unit_qty": (it.get("UnitQty") or "").strip() or None,
            "quantity": _to_float(it.get("Quantity")),
            "unit_of_measure": (it.get("UnitOfMeasure") or "").strip() or None,
            "is_weighted": _to_bool(it.get("bIsWeighted")),
            "qty_in_package": (it.get("QtyInPackage") or "").strip() or None,
            "item_price": _to_float(it.get("ItemPrice")),
            "unit_price": _to_float(it.get("UnitOfMeasurePrice")),
            "allow_discount": _to_bool(it.get("AllowDiscount")),
            "item_status": (it.get("ItemStatus") or "").strip() or None,
            "item_id": (it.get("ItemId") or "").strip() or None,
            "price_update": _to_datetime(it.get("PriceUpdateDate")),
            # Metadata
            "source_file": meta["source_file"],
            "extraction_timestamp": meta["extraction_timestamp"],
            "last_modified": meta["last_modified"],
            # Keep original for troubleshooting (optional; comment out if not needed)
            "raw": it,
        }
        rows.append(row)

    return rows
