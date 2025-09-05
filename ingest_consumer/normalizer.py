from __future__ import annotations
from typing import Any, Dict, List
from datetime import datetime, timezone

_UNIT_MAP = {
    "100 גרם": "100g",
    "100 ג": "100g",
    "100g": "100g",
    "גרם 100": "100g",
    "ליטר": "liter",
    "liter": "liter",
    "יחידה": "unit",
    "unit": "unit",
}

def _norm_unit(u: str) -> str:
    u = u.strip()
    return _UNIT_MAP.get(u, u)

def _norm_ts(ts: str) -> str:
    try:
        if ts.endswith("Z"):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return ts

def normalize(msg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(msg)

    out["branch"] = str(out["branch"]).strip()

    out["timestamp"] = _norm_ts(out["timestamp"])

    if "items_total" not in out and isinstance(out.get("items_sample"), list):
        out["items_total"] = len(out["items_sample"])

    if isinstance(out.get("items_sample"), list):
        norm_items = []
        for item in out["items_sample"]:
            ni = dict(item)
            if isinstance(ni.get("product"), str):
                ni["product"] = ni["product"].strip()
            if "unit" in ni and isinstance(ni["unit"], str):
                ni["unit"] = _norm_unit(ni["unit"])
            norm_items.append(ni)
        out["items_sample"] = norm_items

    return out
