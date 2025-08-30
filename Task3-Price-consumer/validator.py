from typing import Dict, Any, List

ALLOWED_TYPES = {"pricesFull", "promoFull"}

def _err(msgs: List[str]):
    if msgs:
        raise ValueError("; ".join(msgs))

def validate_doc(doc: Dict[str, Any]) -> None:
    msgs: List[str] = []
    if not isinstance(doc, dict):
        msgs.append("document must be an object")

    provider = doc.get("provider")
    if not isinstance(provider, str) or len(provider.strip()) < 1:
        msgs.append("'provider' is too short")

    branch = doc.get("branch")
    if not isinstance(branch, str) or len(branch.strip()) < 1:
        msgs.append("'branch' is too short")

    doc_type = doc.get("type")
    if doc_type not in ALLOWED_TYPES:
        msgs.append(f"'type' must be one of {sorted(ALLOWED_TYPES)}")

    ts = doc.get("timestamp")
    if not isinstance(ts, str) or not ts.endswith("Z"):
        msgs.append("'timestamp' must be ISO8601 UTC with Z suffix")

    items = doc.get("items")
    if not isinstance(items, list) or len(items) < 1:
        msgs.append("'items' must be a non-empty array")
    else:
        for i, it in enumerate(items):
            if not isinstance(it, dict):
                msgs.append(f"item[{i}] must be an object")
                continue
            if not isinstance(it.get("product"), str) or len(it["product"].strip()) < 1:
                msgs.append(f"item[{i}].product is too short")
            if not isinstance(it.get("unit"), str) or len(it["unit"].strip()) < 1:
                msgs.append(f"item[{i}].unit is too short")
            try:
                price = float(it.get("price"))
                if price < 0:
                    msgs.append(f"item[{i}].price must be >= 0")
            except Exception:
                msgs.append(f"item[{i}].price must be a number")

    _err(msgs)

def doc_to_rows(doc: Dict[str, Any]):
    """Flatten normalized+validated doc into DB rows."""
    rows = []
    for it in doc["items"]:
        rows.append({
            "provider": doc["provider"],
            "branch": doc["branch"],
            "doc_type": doc["type"],
            "ts": doc["timestamp"],
            "product": it["product"],
            "unit": it["unit"],
            "price": float(it["price"]),
            "src_key": doc.get("src_key"),
            "etag": doc.get("etag"),
        })
    return rows
