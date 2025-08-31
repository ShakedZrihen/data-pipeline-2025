from extractProcess.saveToSqlProcess.utils import to_iso8601_utc, str_or_none, num_or_none
from extractProcess.extract import _clean

def normalize_envelope(doc: dict) -> dict:
    return {
        "provider": _clean(str(doc.get("provider") or "")),
        "branch":   _clean(str(doc.get("branch") or "")),
        "type":     _clean(str(doc.get("type") or "")),
        "timestamp": to_iso8601_utc(doc.get("timestamp")),
    }

def normalize_price_item(env: dict, item: dict) -> dict:
    msg = {
        **env,
        "productId": str_or_none(item.get("productId")),
        "product":   _clean(item.get("product")),
        "price":     num_or_none(item.get("price")),
        "unit":      _clean(item.get("unit")),
        "brand":     _clean(item.get("brand")),
        "itemType":  _clean(item.get("itemType")),
    }
    return msg

def normalize_promo_item(env: dict, item: dict) -> dict:
    msg = {
        **env,
        "productId": str_or_none(item.get("productId")),
        "product":   _clean(item.get("product")),
        "price":     num_or_none(item.get("price")),
        "unit":      num_or_none(item.get("unit")),
        "brand":     _clean(item.get("brand")),
        "itemType":  _clean(item.get("itemType")),
    }
    return msg

def _pad_branch(b: str) -> str:
    if b and b.isdigit() and len(b) < 3:
        return b.zfill(3)
    return b

def normalize_envelope_strict(doc: dict) -> dict:
    env = normalize_envelope(doc)
    env["branch"] = _pad_branch(env.get("branch"))
    return env
