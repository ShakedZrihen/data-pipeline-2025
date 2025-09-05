from typing import Optional, Dict, Any, List
from .brand_normalizer import split_brand_from_name, normalize_spaces, normalize_dashes
_UNKNOWN_VALUES_CF = {
    "לא ידוע",
    "לא-ידוע",
    "לא־ידוע",           
    "לא ידוע",           
    "unknown",
    "n/a",
    "-", "",
}

def _is_unknown_manufacturer(s: Optional[str]) -> bool:
    if not s:
        return True
    s = normalize_spaces(normalize_dashes(str(s))).casefold()
    return s in {v.casefold() for v in _UNKNOWN_VALUES_CF}

def enrich_item(item: Dict[str, Any]) -> Dict[str, Any]:
    it = dict(item)
    name = it.get("name")
    manuf = it.get("manufacturer")

    hint = None if _is_unknown_manufacturer(manuf) else manuf

    clean_name, brand = split_brand_from_name(name, hint)

    if clean_name and clean_name != name:
        it["name"] = clean_name

    if brand and _is_unknown_manufacturer(manuf):
        it["manufacturer"] = brand

    if brand:
        it["brand"] = brand

    return it

def enrich_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload)
    items = payload.get("items") or []
    out["items"] = [enrich_item(it) for it in items]
    return out