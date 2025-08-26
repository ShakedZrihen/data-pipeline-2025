# enrich_stores.py
from __future__ import annotations
import json, re
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

# ── helpers ───────────────────────────────────────────────────────────────────
def _digits(val) -> Optional[str]:
    """Extract the first integer from mixed text/number, strip leading zeros."""
    if val is None:
        return None
    m = re.search(r'\d+', str(val))
    return (m.group(0).lstrip('0') or '0') if m else None

def _addr_to_text(addr) -> Optional[str]:
    """
    Address can be a plain string or an object. Try to build a simple text line.
    """
    if not addr:
        return None
    if isinstance(addr, str):
        return addr.strip() or None
    if isinstance(addr, dict):
        street = addr.get("Street") or addr.get("street") or addr.get("StreetName")
        house  = (addr.get("House") or addr.get("house") or addr.get("house_number") or
                  addr.get("BuildingNumber") or addr.get("number"))
        city   = addr.get("City") or addr.get("city")
        parts = [str(p).strip() for p in (street, house, city) if p]
        return " ".join(parts) or None
    return None

def _extract_city(store: dict) -> Optional[str]:
    city = store.get("City") or store.get("city")
    if city:
        return str(city).strip() or None
    # maybe inside Address/Location objects
    for key in ("Address", "address", "Location", "location"):
        v = store.get(key)
        if isinstance(v, dict):
            c = v.get("City") or v.get("city")
            if c:
                return str(c).strip() or None
    return None

# ── 1) Build store index from ./stores/<provider>/*.json ─────────────────────
def build_store_index(stores_root: str | Path) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    returns { provider_lower: { branch_id: {"branch_name","address","city"} } }
    """
    root = Path(stores_root)
    idx: Dict[str, Dict[str, Dict[str, str]]] = {}

    for provider_dir in root.iterdir():
        if not provider_dir.is_dir():
            continue
        provider = provider_dir.name.lower()

        for jf in provider_dir.rglob("*.json"):
            try:
                data = json.loads(jf.read_text(encoding="utf-8"))
            except Exception:
                continue

            stores = None
            try:
                stores = data["Root"]["SubChains"]["SubChain"]["Stores"]["Store"]
            except Exception:
                if isinstance(data, dict) and "Stores" in data:
                    stores = data["Stores"].get("Store")

            if not stores:
                continue
            if not isinstance(stores, list):
                stores = [stores]

            for s in stores:
                sid = (s.get("StoreID") or s.get("StoreId") or s.get("StoreNo") or
                       s.get("StoreCode") or s.get("id") or s.get("branch") or s.get("Branch"))
                bid = _digits(sid)
                if not bid:
                    continue

                name = (s.get("StoreName") or s.get("Name") or s.get("name") or s.get("branch_name"))
                addr = (s.get("Address") or s.get("address") or
                        (s.get("Location") or {}).get("Address") or
                        (s.get("location") or {}).get("address"))
                addr_text = _addr_to_text(addr)
                city = _extract_city(s)

                idx.setdefault(provider, {})[bid] = {
                    "branch_name": name if (name and str(name).strip()) else None,
                    "address": addr_text,
                    "city": city,
                }

    return idx

# ── 2) Enrich a single normalized document ────────────────────────────────────
def enrich_doc(doc: Dict[str, Any],
               store_idx: dict,
               overwrite: bool = False,
               normalize_branch: bool = True) -> Tuple[Dict[str, Any], bool]:
    """
    Adds branch_name, address, city based on provider(+branch).
    If normalize_branch=True, rewrite doc['branch'] to numeric-only if possible.
    overwrite=False → don't overwrite existing values in doc.
    Returns (doc, changed).
    """
    provider = (doc.get("provider") or doc.get("Provider") or "").lower()
    changed = False

    # Normalize branch field to digits only
    bid = _digits(doc.get("branch") or doc.get("branch_number") or
                  doc.get("branchNum") or doc.get("StoreID"))
    if normalize_branch and bid and str(doc.get("branch")) != bid:
        doc["branch"] = bid
        changed = True

    if provider not in store_idx:
        return doc, changed

    meta = store_idx[provider].get(bid) if bid else None
    # Fallback: if only one store under provider, use it
    if not meta and len(store_idx[provider]) == 1:
        meta = next(iter(store_idx[provider].values()))
    if not meta:
        return doc, changed

    if (overwrite or not doc.get("branch_name")) and meta.get("branch_name"):
        doc["branch_name"] = meta["branch_name"]; changed = True
    if (overwrite or not doc.get("address")) and meta.get("address"):
        doc["address"] = meta["address"]; changed = True
    if (overwrite or not doc.get("city")) and meta.get("city"):
        doc["city"] = meta["city"]; changed = True

    return doc, changed

# ── 3) Enrich all JSON files under normalize_json ─────────────────────────────
def enrich_dir(normalize_dir: str | Path,
               stores_dir: str | Path,
               out_dir: Optional[str | Path] = None,
               overwrite: bool = False,
               normalize_branch: bool = True) -> int:
    """
    Enriches every *.json under normalize_dir and writes result.
    out_dir=None → in-place update. Returns number of files changed.
    """
    store_idx = build_store_index(stores_dir)
    src = Path(normalize_dir)

    out_root = None if out_dir is None else Path(out_dir)
    if out_root is not None:
        out_root.mkdir(parents=True, exist_ok=True)

    updated = 0
    for jf in src.rglob("*.json"):
        try:
            doc = json.loads(jf.read_text(encoding="utf-8"))
        except Exception:
            continue

        enriched, changed = enrich_doc(
            doc, store_idx, overwrite=overwrite, normalize_branch=normalize_branch
        )
        if not changed:
            continue

        if out_root is None:
            jf.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            (out_root / jf.name).write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")
        updated += 1

    return updated

# Manual run example
if __name__ == "__main__":
    print("updated:", enrich_dir("./normalize_json", "./stores", overwrite=True))
