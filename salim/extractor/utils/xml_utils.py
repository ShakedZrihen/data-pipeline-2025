import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union, Tuple

# ---------- helpers ----------
def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    s = (elem.text or "").strip()
    return s or None

def _iter_children(parent: Optional[ET.Element], name: str):
    if parent is None:
        return []
    for ch in list(parent):
        if _strip_ns(ch.tag) == name:
            yield ch

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
    s = s.replace("₪", "").replace(",", "").replace("\xa0", " ").strip()
    s = s.split()[0]
    try:
        return float(s)
    except Exception:
        return None

def _to_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s = s.strip()
    try:
        return int(s)
    except Exception:
        return None

def _to_bool01(s: Optional[str]) -> Optional[bool]:
    if s is None:
        return None
    s = s.strip()
    if s in {"0", "1"}:
        return s == "1"
    return None

def _parse_dt_flex(s: Optional[str]) -> Optional[str]:
    """Accept 'YYYY-MM-DD HH:MM[:SS]' (no tz) -> ISO Z."""
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            pass
    return None

def _parse_dt_local(s: Optional[str]) -> Optional[str]:
    """Kept for prices XML ('YYYY-MM-DD HH:MM:SS')."""
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None

def _combine_date_time(d: Optional[str], h: Optional[str]) -> Optional[str]:
    if not d:
        return None
    h = h or "00:00:00"
    if len(h) == 5:  
        h = f"{h}:00"
    return _parse_dt_flex(f"{d} {h}")

def _normalize_unit(unit_of_measure: Optional[str], is_weighted: Optional[bool]) -> Optional[str]:
    if not unit_of_measure:
        return "kg" if is_weighted else None
    u = unit_of_measure.strip().lower()
    if "ק\"ג" in unit_of_measure or "קג" in unit_of_measure or "קילוגר" in unit_of_measure or "kg" in u:
        return "kg"
    if "100 גרם" in unit_of_measure or "100גרם" in unit_of_measure or "100g" in u:
        return "100g"
    if "יח" in unit_of_measure or "unit" in u or "each" in u:
        return "unit"
    return unit_of_measure

# ---------- PRICES ----------
def _parse_prices_items(root: ET.Element) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    items: List[Dict[str, Any]] = []
    store_id = _text(_find(root, "StoreId"))  

    items_container = _find(root, "Items")
    candidates: List[ET.Element] = []
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
        is_w   = _to_bool01(_text(_find_direct(node, "bIsWeighted")) or _text(_find(node, "bIsWeighted")))
        itype  = _text(_find_direct(node, "ItemType")) or _text(_find(node, "ItemType"))
        itype_i = int(itype) if itype and itype.isdigit() else None
        manuf  = _text(_find_direct(node, "ManufacturerName")) or _text(_find(node, "ManufacturerName"))
        itemid = _text(_find_direct(node, "ItemId")) or _text(_find(node, "ItemId"))
        upd    = _parse_dt_local(_text(_find_direct(node, "PriceUpdateDate")) or _text(_find(node, "PriceUpdateDate")))

        unit = _normalize_unit(unit_m, is_w)

        if name or price is not None:
            items.append({
                "code": code,
                "name": name,
                "price": price,
                "unit": unit,
                "qty": qty, 
                "unit_price": unit_p,
                "is_weighted": is_w,
                "type": itype_i,
                "manufacturer": manuf,
                "item_id": itemid,
                "updated_at": upd,
            })

    return items, store_id

# ---------- PROMOS ----------
def _parse_promotions_items(root: ET.Element) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    out: List[Dict[str, Any]] = []
    store_id = _text(_find(root, "StoreId"))
    promos = _find(root, "Promotions")
    if promos is None:
        return out, store_id

    for p in _iter_children(promos, "Promotion"):
        promo_id = _text(_find_direct(p, "PromotionId"))
        desc     = _text(_find_direct(p, "PromotionDescription"))
        upd_at   = _parse_dt_flex(_text(_find_direct(p, "PromotionUpdateDate")))
        start_at = _combine_date_time(_text(_find_direct(p, "PromotionStartDate")),
                                      _text(_find_direct(p, "PromotionStartHour")))
        end_at   = _combine_date_time(_text(_find_direct(p, "PromotionEndDate")),
                                      _text(_find_direct(p, "PromotionEndHour")))
        reward_type = _to_int(_text(_find_direct(p, "RewardType")))
        allow_mult  = _to_bool01(_text(_find_direct(p, "AllowMultipleDiscounts")))
        is_weighted = _to_bool01(_text(_find_direct(p, "IsWeightedPromo")))
        min_qty     = _to_float(_text(_find_direct(p, "MinQty")))
        max_qty     = _to_float(_text(_find_direct(p, "MaxQty")))
        min_purchase= _to_float(_text(_find_direct(p, "MinPurchaseAmnt")))
        disc_price  = _to_float(_text(_find_direct(p, "DiscountedPrice")))
        disc_unit   = _to_float(_text(_find_direct(p, "DiscountedPricePerMida")))
        disc_rate   = _to_int(_text(_find_direct(p, "DiscountRate")))
        disc_type   = _to_int(_text(_find_direct(p, "DiscountType")))
        discount_rate_pct = (disc_rate / 100.0) if disc_rate is not None else None

        club_ids: List[int] = []
        clubs = _find_direct(p, "Clubs")
        if clubs is not None:
            for c in _iter_children(clubs, "ClubId"):
                ci = _to_int(_text(c))
                if ci is not None:
                    club_ids.append(ci)

        remarks: List[str] = []
        rems = _find_direct(p, "Remarks")
        if rems is not None:
            for r in _iter_children(rems, "Remark"):
                t = _text(r)
                if t:
                    remarks.append(t)

        items_container = _find_direct(p, "PromotionItems")
        if items_container is None:
            out.append({
                "promotion_id": promo_id,
                "code": None,
                "item_type": None,
                "is_gift": None,
                "description": desc,
                "start_at": start_at,
                "end_at": end_at,
                "updated_at": upd_at,
                "reward_type": reward_type,
                "allow_multiple": allow_mult,
                "is_weighted_promo": is_weighted,
                "min_qty": min_qty,
                "max_qty": max_qty,
                "min_purchase_amount": min_purchase,
                "discounted_price": disc_price,
                "discounted_unit_price": disc_unit,
                "discount_rate_pct": discount_rate_pct,
                "discount_type": disc_type,
                "club_ids": club_ids or None,
                "remarks": remarks or None,
            })
            continue

        for it in _iter_children(items_container, "Item"):
            code = _text(_find_direct(it, "ItemCode"))
            item_type = _to_int(_text(_find_direct(it, "ItemType")))
            is_gift = _to_bool01(_text(_find_direct(it, "IsGiftItem")))
            out.append({
                "promotion_id": promo_id,
                "code": code,
                "item_type": item_type,
                "is_gift": is_gift,
                "description": desc,
                "start_at": start_at,
                "end_at": end_at,
                "updated_at": upd_at,
                "reward_type": reward_type,
                "allow_multiple": allow_mult,
                "is_weighted_promo": is_weighted,
                "min_qty": min_qty,
                "max_qty": max_qty,
                "min_purchase_amount": min_purchase,
                "discounted_price": disc_price,
                "discounted_unit_price": disc_unit,
                "discount_rate_pct": discount_rate_pct,
                "discount_type": disc_type,
                "club_ids": club_ids or None,
                "remarks": remarks or None,
            })

    return out, store_id

# ---------- entry point ----------
def parse_xml_items(xml_data: Union[str, bytes]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Returns (items, store_id), auto-detecting Prices vs Promotions XML.
    """
    if isinstance(xml_data, bytes):
        root = ET.fromstring(xml_data)
    else:
        root = ET.fromstring(xml_data.encode("utf-8", errors="ignore"))

    if _find(root, "Promotions") is not None:
        return _parse_promotions_items(root)
    return _parse_prices_items(root)

def iso_from_filename(fname: str) -> str:
    """
    Accepts price_YYYYMMDD_HHMMSS.gz or promoFull_YYYYMMDDHHMM.gz (12 digits).
    Falls back to now UTC.
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
