import os
from datetime import datetime, timezone
from typing import Any, Dict, List

DEFAULT_UNIT = os.getenv("DEFAULT_UNIT", "unit")
DEFAULT_BRANCH = os.getenv("DEFAULT_BRANCH", "default").strip()
ALLOWED_TYPES = {"pricesFull", "promoFull"}

def _to_utc_z(ts: str) -> str:
    s = ts.strip()
    if s.endswith("Z"):
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    else:
        dt = datetime.fromisoformat(s)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def _norm_text(v: Any) -> str:
    return ("" if v is None else str(v)).strip()

def _norm_price(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0

def _canon_type(t: str) -> str:
    t = (t or "").strip()
    return t if t in ALLOWED_TYPES else "pricesFull"

def normalize_message(msg: Dict[str, Any]) -> Dict[str, Any]:
    provider = _norm_text(msg.get("provider")).lower()
    branch = _norm_text(msg.get("branch")) or DEFAULT_BRANCH
    doc_type = _canon_type(_norm_text(msg.get("type")))
    raw_ts = _norm_text(msg.get("timestamp")) or datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    ts = _to_utc_z(raw_ts)

    items: List[Dict[str, Any]] = []
    for it in msg.get("items", []):
        items.append({
            "product": _norm_text(it.get("product")),
            "unit": _norm_text(it.get("unit")) or DEFAULT_UNIT,
            "price": _norm_price(it.get("price")),
        })

    return {
        "provider": provider,
        "branch": branch,
        "type": doc_type,
        "timestamp": ts,
        "items": items,
        "src_key": _norm_text(msg.get("src_key")),
        "etag": _norm_text(msg.get("etag")),
    }
