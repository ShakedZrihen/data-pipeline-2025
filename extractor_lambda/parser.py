import gzip, io, json, csv
from xml.etree import ElementTree as ET
import re
import logging
import xml.etree.ElementTree as ET
logger = logging.getLogger("extractor")

# ------------  XML &  ---------------

def _has_xml_decl(bs: bytes) -> bool:
    head = bs[:200].lstrip().lower()
    return head.startswith(b'<?xml')

def _xml_root_robust(xml_bytes: bytes) -> ET.Element:
    """
    טוען XML מהבייטים בלי לאבד עברית.
    אם יש הצהרת encoding – נסמוך עליה.
    אם אין – ננסה ידנית: utf-8, cp1255 (Windows-1255), iso-8859-8.
    """
    if _has_xml_decl(xml_bytes):
        return ET.fromstring(xml_bytes)

    for enc in ("utf-8", "cp1255", "iso-8859-8"):
        try:
            txt = xml_bytes.decode(enc, errors="strict")
            return ET.fromstring(txt)
        except Exception:
            continue

    return ET.fromstring(xml_bytes)

def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def _text(el):
    return (el.text or "").strip() if el is not None else ""

def _first_text_by_candidates(node, candidates):
    for c in candidates:
        for child in list(node):
            if _strip_ns(child.tag).lower() == c.lower():
                t = _text(child)
                if t:
                    return t
    return ""

def _findall_by_localname(root, name):
    out = []
    lname = name.lower()
    for elem in root.iter():
        if _strip_ns(elem.tag).lower() == lname:
            out.append(elem)
    return out

# ------------- Parser Price & Promo ----------------

def parse_price_xml(xml_bytes: bytes):
    items_out = []
    root = _xml_root_robust(xml_bytes)

    # איתור צמתים
    item_nodes = []
    for name in ("Item", "Product", "ProductItem", "PriceItem"):
        item_nodes = _findall_by_localname(root, name)
        if item_nodes:
            break
    if not item_nodes:
        for cont in ("Items", "Products"):
            for container in _findall_by_localname(root, cont):
                for name in ("Item", "Product", "ProductItem", "PriceItem"):
                    item_nodes = [n for n in list(container)
                                  if _strip_ns(n.tag).lower() == name.lower()]
                    if item_nodes:
                        break
            if item_nodes:
                break

    name_tags  = ("ItemName", "ProductName", "Name", "ItemDesc", "Description")
    price_tags = ("ItemPrice", "Price", "UnitPrice", "CurrentPrice")
    unit_tags  = ("Unit", "UnitOfMeasure", "UnitQty", "Quantity", "QuantityInPackage")

    for node in item_nodes:
        pname = _first_text_by_candidates(node, name_tags)
        punit = _first_text_by_candidates(node, unit_tags)
        pprice_txt = _first_text_by_candidates(node, price_tags)

        pprice = None
        if pprice_txt:
            pprice_txt = pprice_txt.replace("₪", "").replace(",", "").strip()
            try:
                pprice = float(pprice_txt)
            except Exception:
                pprice = None

        if pname or pprice is not None:
            items_out.append({"product": pname, "price": pprice, "unit": punit})
    return items_out

def parse_promo_xml(xml_bytes: bytes):
    items_out = []
    root = _xml_root_robust(xml_bytes)

    promo_nodes = []
    for name in ("Promotion", "Promo", "Deal"):
        promo_nodes = _findall_by_localname(root, name)
        if promo_nodes:
            break
    if not promo_nodes:
        for cont in ("Promotions", "Deals", "Promos"):
            for container in _findall_by_localname(root, cont):
                for name in ("Promotion", "Promo", "Deal"):
                    promo_nodes = [n for n in list(container)
                                   if _strip_ns(n.tag).lower() == name.lower()]
                    if promo_nodes:
                        break
            if promo_nodes:
                break

    desc_tags  = ("PromotionDescription", "Description", "Name", "Title")
    price_tags = ("Price", "PromoPrice", "BenefitPrice", "ItemPrice")
    unit_tags  = ("Unit", "UnitOfMeasure")

    for node in promo_nodes:
        desc = _first_text_by_candidates(node, desc_tags)
        price_txt = _first_text_by_candidates(node, price_tags)
        unit = _first_text_by_candidates(node, unit_tags)

        price = None
        if price_txt:
            price_txt = price_txt.replace("₪", "").replace(",", "").strip()
            try:
                price = float(price_txt)
            except Exception:
                price = None

        if desc or price is not None:
            items_out.append({"product": desc, "price": price, "unit": unit})
    return items_out


def looks_like_xml(b: bytes) -> bool:
    head = b[:200].lstrip()
    return head.startswith(b'<?xml') or head.startswith(b'<')

def parse_content(buf: bytes, kind_hint: str | None = None):
    """
    מחזיר ("xml"|"bin", rows)
    kind_hint: 'pricesFull' / 'promoFull' (מרמז מה לנסות קודם)
    """
    if not looks_like_xml(buf):
        logger.warning("Not XML? parsing skipped.")
        return "bin", []

    kh = (kind_hint or "").lower()
    if kh == "promofull":
        rows = parse_promo_xml(buf) or parse_price_xml(buf)
    else:
        rows = parse_price_xml(buf) or parse_promo_xml(buf)
    return "xml", rows

def decompress_gz_to_bytes(obj_body: bytes) -> bytes:
    with gzip.GzipFile(fileobj=io.BytesIO(obj_body)) as gz:
        return gz.read()

def sniff_format(buf: bytes) -> str:
    head = buf[:1024].lstrip()
    if head.startswith(b"{") or head.startswith(b"["):
        return "json"
    if head.startswith(b"<"):
        return "xml"
    text = buf.decode("utf-8", errors="ignore")
    if "," in text and "\n" in text:
        return "csv"
    if "\t" in text:
        return "tsv"
    return "txt"

def parse_content(buf: bytes):
    fmt = sniff_format(buf)
    if fmt == "json":
        data = json.loads(buf.decode("utf-8"))
        if isinstance(data, dict) and "items" in data:
            rows = data["items"]
        elif isinstance(data, list):
            rows = data
        else:
            rows = [data]
        return fmt, rows
    elif fmt == "xml":
        root = ET.fromstring(buf.decode("utf-8", errors="ignore"))
        rows = []
        for item in root.findall(".//Item"):
            rows.append({
                "product": item.findtext("ItemName", ""),
                "price": item.findtext("ItemPrice", ""),
                "unit": item.findtext("UnitQty", ""),
            })
        return fmt, rows
    elif fmt in ("csv", "tsv"):
        delimiter = "," if fmt == "csv" else "\t"
        text = buf.decode("utf-8", errors="ignore")
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        return fmt, list(reader)
    else:
        text = buf.decode("utf-8", errors="ignore")
        return fmt, [line for line in text.splitlines() if line.strip()]

_ws_re = re.compile(r"\s+", re.UNICODE)

def _clean_text(s):
    if s is None:
        return ""
    s = str(s)
    s = _ws_re.sub(" ", s).strip()
    return s

def normalize_rows(rows):
    out = []
    for r in rows:
        name = _clean_text(r.get("product"))
        unit = _clean_text(r.get("unit"))
        price = r.get("price")

        if price is None or price == "":
            ptxt = _clean_text(r.get("price"))
            ptxt = ptxt.replace("₪", "").replace(",", "")
            try:
                price = float(ptxt) if ptxt else None
            except Exception:
                price = None

        if name or price is not None:
            out.append({"product": name, "price": price, "unit": unit or ""})
    return out