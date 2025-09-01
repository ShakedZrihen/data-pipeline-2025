import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# TODO: need to correct all of this ! . 

def _strip_ns(tag: str) -> str:
    # remove XML namespace like {urn:...}Tag -> Tag
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag

def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    s = (elem.text or "").strip()
    return s or None

def _find(root: ET.Element, tag: str) -> Optional[ET.Element]:
    for ch in root.iter():
        if _strip_ns(ch.tag) == tag:
            return ch
    return None

def _find_direct(parent: ET.Element, tag: str) -> Optional[ET.Element]:
    for ch in list(parent):
        if _strip_ns(ch.tag) == tag:
            return ch
    return None


def _to_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    # remove currency symbol, thousands separators, Hebrew spaces, etc.
    s = s.replace("₪", "").replace(",", "").replace("\xa0", " ").strip()
    # take first token if needed (e.g., "12.90 ש\"ח")
    s = s.split()[0]
    try:
        return float(s)
    except Exception:
        return None


def _to_bool_int(s: Optional[str]) -> Optional[bool]:
    if s is None:
        return None
    s = s.strip()
    if s in {"0", "1"}:
        return s == "1"
    return None

def _parse_dt_local(s: Optional[str]) -> Optional[str]:
    """
    Input like '2025-09-01 06:50:51' (no tz). Return ISO8601 Z.
    """
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        # Assume local → convert to UTC if you know timezone; otherwise mark as Z-naive.
        # Here we treat it as UTC to keep a consistent timeline.
        return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None

def _normalize_unit(unit_of_measure: Optional[str], is_weighted: Optional[bool]) -> Optional[str]:
    if not unit_of_measure:
        return None
    u = unit_of_measure.strip().lower()
    # Hebrew / English common forms
    if "ק\"ג" in unit_of_measure or "קג" in unit_of_measure or "קילוגר" in unit_of_measure or "kg" in u:
        return "kg"
    if "100 גרם" in unit_of_measure or "100גרם" in unit_of_measure or "100g" in u:
        return "100g"
    if "יח" in unit_of_measure or "unit" in u or "each" in u:
        return "unit"
    # fallback: if weighted and quantity=1 by kg, keep kg
    if is_weighted:
        return "kg"
    return unit_of_measure  # last resort: keep original




def parse_xml_items(xml_text: bytes | str) -> List[Dict[str, Any]]:
    """
    Parse your price XML into a list of normalized item dicts.
    Output fields (JSON-safe):
      code (str), name (str), price (float), unit (str), qty (float|None),
      unit_price (float|None), is_weighted (bool|None), type (int|None),
      manufacturer (str|None), country (str|None), item_id (str|None),
      updated_at (ISO8601 Z)
    """
    items: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        print("XML parse error:", e)
        return items

    # Find <Items>...</Items> and iterate its direct <Item> children (if present);
    # otherwise, fallback to scanning all descendants named Item.
    items_container = _find(root, "Items")
    candidates = []
    if items_container is not None:
        for ch in list(items_container):
            if _strip_ns(ch.tag) == "Item":
                candidates.append(ch)
    else:
        for node in root.iter():
            if _strip_ns(node.tag) == "Item":
                candidates.append(node)

    for node in candidates:
        code   = _text(_find_direct(node, "ItemCode")) or _text(_find(node, "ItemCode"))
        name   = _text(_find_direct(node, "ItemName")) or _text(_find(node, "ItemName"))
        price  = _to_float(_text(_find_direct(node, "ItemPrice")) or _text(_find(node, "ItemPrice")))
        unit_m = _text(_find_direct(node, "UnitOfMeasure")) or _text(_find(node, "UnitOfMeasure"))
        qty    = _to_float(_text(_find_direct(node, "Quantity")) or _text(_find(node, "Quantity")))
        unit_p = _to_float(_text(_find_direct(node, "UnitOfMeasurePrice")) or _text(_find(node, "UnitOfMeasurePrice")))
        is_w   = _to_bool_int(_text(_find_direct(node, "bIsWeighted")) or _text(_find(node, "bIsWeighted")))
        itype  = _text(_find_direct(node, "ItemType")) or _text(_find(node, "ItemType"))
        itype_i = int(itype) if itype and itype.isdigit() else None
        manuf  = _text(_find_direct(node, "ManufacturerName")) or _text(_find(node, "ManufacturerName"))
        cntry  = _text(_find_direct(node, "ManufactureCountry")) or _text(_find(node, "ManufactureCountry"))
        itemid = _text(_find_direct(node, "ItemId")) or _text(_find(node, "ItemId"))
        upd    = _parse_dt_local(_text(_find_direct(node, "PriceUpdateDate")) or _text(_find(node, "PriceUpdateDate")))

        unit = _normalize_unit(unit_m, is_w)

        # Only include records that have at least a name or price
        if name or price is not None:
            items.append({
                "code": code,
                "name": name,
                "price": price,
                "unit": unit,
                "qty": qty,
                "unit_price": unit_p,
                "is_weighted": is_w,
                "type": itype_i,  # 0=produce/weighted, 1=packaged (per your sample)
                "manufacturer": manuf,
                "country": cntry,
                "item_id": itemid,
                "updated_at": upd,
            })

    return items

def iso_from_filename(fname: str) -> str:
    """
    Convert price_YYYYMMDD_HHMMSS.gz → ISO time (Z).
    If not found → now (UTC) in ISO Z.
    """
    m = re.search(r"(\d{8}_\d{6}|\d{12})", fname)
    if not m:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    s = m.group(1)
    try:
        if "_" in s:
            dt = datetime.strptime(s, "%Y%m%d_%H%M%S")
        else:
            dt = datetime.strptime(s, "%Y%m%d%H%M")
        return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
