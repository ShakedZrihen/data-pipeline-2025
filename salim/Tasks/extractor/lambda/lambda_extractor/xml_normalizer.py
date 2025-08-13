# extractor/xml_normalizer.py
import gzip
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

def _text(el, *names):
    if el is None:
        return None
    for n in names:
        x = el.find(n)
        if x is not None:
            t = (x.text or "").strip()
            if t:
                return t
    return None

def _to_float(val):
    try:
        return float(str(val).replace(",", "."))
    except Exception:
        return None

def parse_gz_xml_bytes(gz_bytes: bytes) -> ET.Element:
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb") as gz:
        xml_bytes = gz.read()
    return ET.fromstring(xml_bytes)

def extract_prices_payload(root, provider, branch, ts_str):
    items_el = root.find("Items")
    items = []
    if items_el is not None:
        for it in items_el.findall("Item"):
            name = _text(it, "ItemNm", "ItemName")
            price = _to_float(_text(it, "ItemPrice"))
            unit = _text(it, "UnitOfMeasure", "UnitQty")
            if name and price is not None:
                items.append({"product": name, "price": price, "unit": unit or ""})
    return {
        "provider": provider,
        "branch": branch,
        "type": "pricesFull",
        "timestamp": ts_str,
        "items": items,
    }

def extract_promos_payload(root, provider, branch, ts_str):
    proms_el = root.find("Promotions")
    items = []
    if proms_el is not None:
        for pr in proms_el.findall("Promotion"):
            desc = _text(pr, "PromotionDescription")
            price = _to_float(_text(pr, "DiscountedPrice", "DiscountedPricePerMida"))
            if desc and price is not None:
                items.append({"product": desc, "price": price, "unit": ""})
    return {
        "provider": provider,
        "branch": branch,
        "type": "promoFull",
        "timestamp": ts_str,
        "items": items,
    }

def build_payload(file_type, root, provider, branch, timestamp):
    if isinstance(timestamp, datetime):
        ts_str = timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        ts_str = str(timestamp)

    if file_type == "pricesFull":
        return extract_prices_payload(root, provider, branch, ts_str)
    elif file_type == "promoFull":
        return extract_promos_payload(root, provider, branch, ts_str)
    raise ValueError(f"Unsupported file_type: {file_type}")
