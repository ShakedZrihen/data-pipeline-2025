import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import  Optional

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