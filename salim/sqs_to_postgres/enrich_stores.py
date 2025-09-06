from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, Iterable
from consts import *

def digits(val) -> Optional[str]:
    if val is None:
        return None
    m = re.search(r'\d+', str(val))
    return (m.group(0).lstrip('0') or '0') if m else None

def addr_to_text(addr) -> Optional[str]:
    if not addr:
        return None
    if isinstance(addr, str):
        s = addr.strip()
        return s or None
    if isinstance(addr, dict):
        street = addr.get("Street") or addr.get("street") or addr.get("StreetName")
        house  = (addr.get("House") or addr.get("house") or addr.get("house_number") or
                  addr.get("BuildingNumber") or addr.get("number"))
        city   = addr.get("City") or addr.get("city")
        parts = [str(p).strip() for p in (street, house, city) if p]
        out = " ".join(parts).strip()
        return out or None
    return None

def extract_city(store: dict) -> Optional[str]:
    city = store.get("City") or store.get("city")
    if isinstance(city, str) and city.strip():
        return city.strip()
    for key in ("Address", "address", "Location", "location"):
        v = store.get(key)
        if isinstance(v, dict):
            c = v.get("City") or v.get("city")
            if isinstance(c, str) and c.strip():
                return c.strip()
    return None

def clean_product_name(name: str) -> str:
    if not name:
        return name
    s = str(name)

    s = s.replace("×", "x")
    s = re.sub(r"[™®©]", " ", s)
    s = re.sub(rf"\b{CODE_WORDS}\s*[:#]?\s*\d+\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\b\d{7,14}\b", " ", s)
    s = re.sub(r"\b\d+\s*[xX]\s*\d+\b", " ", s)
    s = re.sub(rf"(?<!\w)\d+(?:[.,]\d+)?\s*{UNITS}(?!\w)", " ", s)
    s = re.sub(r"(?<!\w)\d+\s*(?:יחידות|יח(?:[\"'׳’״])?)\b", " ", s)

    def kill_brackets(text: str) -> str:
        def repl(m):
            inside = m.group(1)
            if re.fullmatch(
                rf"\s*(?:{MARKETING}|{PACK_WORDS}|{CODE_WORDS}|\d+(?:[.,]\d+)?\s*{UNITS}|\d{{7,14}})\s*",
                inside, flags=re.IGNORECASE
            ):
                return " "
            return m.group(0)
        text = re.sub(r"\(([^)]{0,80})\)", repl, text)
        text = re.sub(r"\[([^\]]{0,80})\]", repl, text)
        text = re.sub(r"\{([^}]{0,80})\}", repl, text)
        return text
    s = kill_brackets(s)

    s = re.sub(rf"\b{PACK_WORDS}\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(rf"\b{MARKETING}\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"(?:^|\s)\d+(?:[.,]\d+)?\s*$", " ", s)
    s = re.sub(r"\s+(?:עם|בטעם)\s+[A-Za-zא-ת]\.?$", "", s)
    s = re.sub(r"(?:^|\s)[-–—]?[A-Za-zא-ת](?:[.'׳’\"״])?\s*$", " ", s)
    s = re.sub(r"[·•|/\\]+", " ", s)
    s = re.sub(r"\s*[-–—]{2,}\s*", " ", s)
    s = re.sub(r"\s*[-–—]\s*", " - ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    s = re.sub(r"^[,\-–—\s]+|[,\-–—\s]+$", "", s)

    return s

def clean_product_field_in_doc(doc: Dict[str, Any]) -> bool:
    changed = False

    if isinstance(doc.get("product"), str) and doc["product"].strip():
        new = clean_product_name(doc["product"])
        if new and new != doc["product"]:
            doc["product"] = new
            changed = True

    items = None
    for k in ("items", "products", "entries", "rows", "lines", "data"):
        v = doc.get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            items = v
            break
    if items:
        for it in items:
            if not isinstance(it, dict):
                continue
            if isinstance(it.get("product"), str) and it["product"].strip():
                new = clean_product_name(it["product"])
                if new and new != it["product"]:
                    it["product"] = new
                    changed = True
    return changed

def iter_stores_in_data(data: dict) -> Iterable[dict]:
    try:
        subchains = data["Root"]["SubChains"]["SubChain"]
        if isinstance(subchains, dict):
            subchains = [subchains]
        for sub in subchains:
            stores = sub["Stores"]["Store"]
            if isinstance(stores, dict):
                stores = [stores]
            for s in stores:
                yield s
        return
    except Exception:
        pass
    try:
        stores = data["Stores"]["Store"]
        if isinstance(stores, dict):
            stores = [stores]
        for s in stores:
            yield s
        return
    except Exception:
        pass


def build_store_index(stores_root: str | Path) -> Dict[str, Dict[str, Dict[str, str]]]:
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

            for s in iter_stores_in_data(data):
                sid = (s.get("StoreID") or s.get("StoreId") or s.get("StoreNo") or
                       s.get("StoreCode") or s.get("id") or s.get("branch") or s.get("Branch"))
                bid = digits(sid)
                if not bid:
                    continue

                name = (s.get("StoreName") or s.get("Name") or s.get("name") or s.get("branch_name"))
                addr = (s.get("Address") or s.get("address") or
                        (s.get("Location") or {}).get("Address") or
                        (s.get("location") or {}).get("address"))
                idx.setdefault(provider, {})[bid] = {
                    "branch_name": (name.strip() if isinstance(name, str) and name.strip() else None),
                    "address": addr_to_text(addr),
                    "city": extract_city(s),
                }
    return idx

def enrich_doc(doc: Dict[str, Any],
               store_idx: dict,
               overwrite: bool = False,
               normalize_branch: bool = True) -> Tuple[Dict[str, Any], bool]:

    provider = (doc.get("provider") or doc.get("Provider") or "").lower()
    changed = False

    bid = digits(doc.get("branch") or doc.get("branch_number") or
                  doc.get("branchNum") or doc.get("StoreID"))
    if normalize_branch and bid and str(doc.get("branch")) != bid:
        doc["branch"] = bid
        changed = True

    if provider in store_idx:
        meta = store_idx[provider].get(bid) if bid else None
        if not meta and len(store_idx[provider]) == 1:
            meta = next(iter(store_idx[provider].values()))
        if meta:
            if (overwrite or not doc.get("branch_name")) and meta.get("branch_name"):
                doc["branch_name"] = meta["branch_name"]; changed = True
            if (overwrite or not doc.get("address")) and meta.get("address"):
                doc["address"] = meta["address"]; changed = True
            if (overwrite or not doc.get("city")) and meta.get("city"):
                doc["city"] = meta["city"]; changed = True

    if clean_product_field_in_doc(doc):
        changed = True

    return doc, changed

def enrich_dir(normalize_dir: str | Path,
               stores_dir: str | Path,
               out_dir: Optional[str | Path] = None,
               overwrite: bool = False,
               normalize_branch: bool = True) -> int:

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
            doc, store_idx,
            overwrite=overwrite,
            normalize_branch=normalize_branch
        )
        if not changed:
            continue

        if out_root is None:
            jf.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            out_path = out_root / jf.relative_to(src)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")
        updated += 1

    return updated
