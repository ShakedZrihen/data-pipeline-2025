# # extractor/xml_normalizer.py
# import gzip
# import io
# import xml.etree.ElementTree as ET
# from datetime import datetime, timezone

# def _text(el, *names):
#     if el is None:
#         return None
#     for n in names:
#         x = el.find(n)
#         if x is not None:
#             t = (x.text or "").strip()
#             if t:
#                 return t
#     return None

# def _to_float(val):
#     try:
#         return float(str(val).replace(",", "."))
#     except Exception:
#         return None

# def parse_gz_xml_bytes(gz_bytes: bytes) -> ET.Element:
#     with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb") as gz:
#         xml_bytes = gz.read()
#     return ET.fromstring(xml_bytes)

# def extract_prices_payload(root, provider, branch, ts_str):
#     items_el = root.find("Items")
#     items = []
#     if items_el is not None:
#         for it in items_el.findall("Item"):
#             name = _text(it, "ItemNm", "ItemName")
#             price = _to_float(_text(it, "ItemPrice"))
#             unit = _text(it, "UnitOfMeasure", "UnitQty")
#             if name and price is not None:
#                 items.append({"product": name, "price": price, "unit": unit or ""})
#     return {
#         "provider": provider,
#         "branch": branch,
#         "type": "pricesFull",
#         "timestamp": ts_str,
#         "items": items,
#     }

# def extract_promos_payload(root, provider, branch, ts_str):
#     proms_el = root.find("Promotions")
#     items = []
#     if proms_el is not None:
#         for pr in proms_el.findall("Promotion"):
#             desc = _text(pr, "PromotionDescription")
#             price = _to_float(_text(pr, "DiscountedPrice", "DiscountedPricePerMida"))
#             if desc and price is not None:
#                 items.append({"product": desc, "price": price, "unit": ""})
#     return {
#         "provider": provider,
#         "branch": branch,
#         "type": "promoFull",
#         "timestamp": ts_str,
#         "items": items,
#     }

# def build_payload(file_type, root, provider, branch, timestamp):
#     if isinstance(timestamp, datetime):
#         ts_str = timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
#     else:
#         ts_str = str(timestamp)

#     if file_type == "pricesFull":
#         return extract_prices_payload(root, provider, branch, ts_str)
#     elif file_type == "promoFull":
#         return extract_promos_payload(root, provider, branch, ts_str)
#     raise ValueError(f"Unsupported file_type: {file_type}")

import gzip, io, xml.etree.ElementTree as ET
from datetime import datetime, timezone

# -------- helpers ----------
def _text_from(el, names):
    if el is None:
        return None
    for n in names:
        x = el.find(n)
        if x is not None:
            t = (x.text or "").strip()
            if t:
                return t
    return None

def _text_anywhere(el, names):
    """Search current element, then any descendants, for first non-empty text under any of names."""
    if el is None:
        return None
    for n in names:
        x = el.find(n)
        if x is not None and (x.text or "").strip():
            return (x.text or "").strip()
    # descendant search
    for n in names:
        x = el.find(".//" + n)
        if x is not None and (x.text or "").strip():
            return (x.text or "").strip()
    return None

def _to_float(val):
    try:
        return float(str(val).replace(",", "."))
    except Exception:
        return None

# -------- core ----------
def parse_gz_xml_bytes(gz_bytes: bytes) -> ET.Element:
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb") as gz:
        xml_bytes = gz.read()

    # strip junk before first '<'
    i = xml_bytes.find(b"<")
    if i > 0:
        xml_bytes = xml_bytes[i:]

    # parse robustly with tolerant decoding fallback
    try:
        return ET.fromstring(xml_bytes)
    except ET.ParseError:
        try:
            text = xml_bytes.decode("utf-8", errors="replace")
        except Exception:
            text = xml_bytes.decode("iso-8859-8", errors="replace")
        return ET.fromstring(text.encode("utf-8"))

def _timestamp_str(ts):
    if isinstance(ts, datetime):
        return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(ts)

def extract_prices_payload(root, provider, branch, ts_str):
    # Accept various layouts:
    #  - Items/Item
    #  - PriceList/Items/Item
    #  - .//Item anywhere
    items = []
    for it in root.findall(".//Item"):
        name  = _text_anywhere(it, ["ItemNm", "ItemName", "ItemDescription", "ItmName"])
        price = _to_float(_text_anywhere(it, ["ItemPrice", "Price", "UnitPrice", "SellPrice", "CurrentPrice"]))
        unit  = _text_anywhere(it, ["UnitOfMeasure", "UnitQty", "QtyInPackage", "Quantity"])
        if name and price is not None:
            items.append({"product": name, "price": price, "unit": unit or ""})
    return {"provider": provider, "branch": branch, "type": "pricesFull", "timestamp": ts_str, "items": items}

def extract_promos_payload(root, provider, branch, ts_str):
    # Accept variants:
    #  - Promotions/Promotion
    #  - .//Promotion or .//Promo
    promos = root.findall(".//Promotion") or root.findall(".//Promo")
    items = []
    for pr in promos:
        # description/name
        desc  = _text_anywhere(pr, ["PromotionDescription", "Description", "PromoDescription", "Name", "Title"])
        # price-like fields common in Israeli promo files
        price = _to_float(_text_anywhere(pr, ["DiscountedPrice", "DiscountedPricePerMida", "FinalPrice", "Price", "BenefitValue"]))
        if desc and price is not None:
            items.append({"product": desc, "price": price, "unit": ""})
    return {"provider": provider, "branch": branch, "type": "promoFull", "timestamp": ts_str, "items": items}

def build_payload(file_type, root, provider, branch, timestamp):
    ts_str = _timestamp_str(timestamp)
    if file_type == "pricesFull":
        return extract_prices_payload(root, provider, branch, ts_str)
    if file_type == "promoFull":
        return extract_promos_payload(root, provider, branch, ts_str)
    # Unknown type -> empty but explicit
    return {"provider": provider, "branch": branch, "type": file_type, "timestamp": ts_str, "items": []}
