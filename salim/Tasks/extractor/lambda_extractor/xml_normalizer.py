
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
    print("Parsing gzipped XML bytes...")
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb") as gz:
        xml_bytes = gz.read()

    # strip junk before first '<'
    i = xml_bytes.find(b"<")
    if i > 0:
        xml_bytes = xml_bytes[i:]

    # parse robustly with tolerant decoding fallback
    try:
        print("Trying to parse XML directly...")
        return ET.fromstring(xml_bytes)
    except ET.ParseError:
        print("ParseError, trying to decode with utf-8...")
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
    items = []
    for it in root.findall(".//Item"):
        name  = _text_anywhere(it, ["ItemNm", "ItemName", "ItemDescription", "ItmName" ,"ManufacturerItemDescription"])
        barcode = _text_anywhere(it, ["ItemCode"])
        price = _to_float(_text_anywhere(it, ["ItemPrice", "Price", "UnitPrice", "SellPrice", "CurrentPrice"]))
        unit  = _text_anywhere(it, ["UnitOfMeasure", "UnitQty", "QtyInPackage", "Quantity"])
        if name and price is not None:
            items.append({"barcode": barcode ,"product": name, "price": price, "unit": unit or ""})
    return {"provider": provider, "branch": branch, "type": "pricesFull", "timestamp": ts_str, "items": items}

def extract_promos_payload(root, provider, branch, ts_str):
    promos = root.findall(".//Promotion") or root.findall(".//Promo")
    items = []
    for pr in promos:
        # description/name
        desc  = _text_anywhere(pr, ["PromotionDescription", "Description", "PromoDescription", "Name", "Title", "ManufacturerItemDescription"])
        barcode = _text_anywhere(pr, ["ItemCode"])
        # price-like fields common in Israeli promo files
        price = _to_float(_text_anywhere(pr, ["DiscountedPrice", "FinalPrice", "Price", "BenefitValue"]))
        unit  = _text_anywhere(pr, ["UnitOfMeasure", "UnitQty", "QtyInPackage", "Quantity"])
        if desc and price is not None:
            items.append({"barcode": barcode, "product": desc, "price": price, "unit":unit or ""})
    return {"provider": provider, "branch": branch, "type": "promoFull", "timestamp": ts_str, "items": items}

def build_payload(file_type, root, provider, branch, timestamp):
    ts_str = _timestamp_str(timestamp)
    if file_type == "pricesFull":
        return extract_prices_payload(root, provider, branch, ts_str)
    if file_type == "promoFull":
        return extract_promos_payload(root, provider, branch, ts_str)

    return {"provider": provider, "branch": branch, "type": file_type, "timestamp": ts_str, "items": []}
