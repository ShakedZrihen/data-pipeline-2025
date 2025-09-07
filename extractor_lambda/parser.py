import gzip, io, json, csv
from xml.etree import ElementTree as ET
import re
import logging
import xml.etree.ElementTree as ET
logger = logging.getLogger("extractor")
PREFERRED_DECODINGS = ("utf-8-sig", "utf-8", "windows-1255", "cp1255", "iso-8859-8")

# ------------  XML & Hebrew encoding fixes ---------------

def _decode_smart(b: bytes) -> str:
    if b.startswith(b'\xef\xbb\xbf'):
        b = b[3:]
    elif b.startswith((b'\xff\xfe', b'\xfe\xff')):
        try:
            return b.decode('utf-16')
        except UnicodeDecodeError:
            pass

    encodings = [
        'utf-8',
        'windows-1255',
        'iso-8859-8',
        'cp1255',
        'utf-16',
        'latin1'
    ]

    for enc in encodings:
        try:
            decoded = b.decode(enc)
            if any(ord(c) >= 0x0590 and ord(c) <= 0x05FF for c in decoded[:1000]) or \
               any(c.isascii() and c.isalnum() for c in decoded[:1000]):
                return decoded
        except (UnicodeDecodeError, UnicodeError):
            continue
    logger.warning("Failed to decode with all Hebrew encodings, using UTF-8 with replacement")
    return b.decode("utf-8", errors="replace")
def _decode_bytes_lossless(b: bytes) -> str:
    last_err = None
    for enc in PREFERRED_DECODINGS:
        try:
            return b.decode(enc)
        except UnicodeDecodeError as e:
            last_err = e
            continue
    return b.decode("latin-1")
def _has_xml_decl(bs: bytes) -> bool:
    head = bs[:200].lstrip().lower()
    return head.startswith(b'<?xml')
# --_xml_root_robust ---
def _xml_root_robust(xml_bytes: bytes):
    try:
        head = xml_bytes[:200].decode("ascii", errors="ignore")
    except Exception:
        head = ""

    enc_decl = None
    if "encoding" in head:
        import re
        m = re.search(r'encoding=["\']([\w\-]+)["\']', head)
        if m:
            enc_decl = m.group(1).lower()

    if enc_decl:
        try:
            text = xml_bytes.decode(enc_decl)
            return ET.fromstring(text)
        except UnicodeDecodeError:
            pass

    for enc in PREFERRED_DECODINGS:
        try:
            text = xml_bytes.decode(enc)
            return ET.fromstring(text)
        except UnicodeDecodeError:
            continue

    try:
        return ET.fromstring(xml_bytes)
    except Exception:
        text = xml_bytes.decode("latin-1")
        return ET.fromstring(text)


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def _text(el):
    if el is None:
        return ""
    text = el.text or ""
    text = re.sub(r'\s+', ' ', text).strip()
    return text

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
def looks_like_xml_bytes(b: bytes) -> bool:
    head = b[:200].lstrip()
    return head.startswith(b'<?xml') or b'<Items' in head or b'<root' in head or b'<item' in head

# ------------- Parser Price & Promo ----------------
def _first_text(node, *cands):
    return _first_text_by_candidates(node, cands)

def _to_float(txt):
    if not txt:
        return None
    t = (txt.replace("₪","")
           .replace('ש"ח','').replace('ש״ח','').replace('שח','')
           .replace(",", "").strip())
    try:
        return float(t)
    except Exception:
        return None

def _split_size(value_txt, unit_txt):
    v = None
    if value_txt:
        try:
            v = float(str(value_txt).strip())
        except Exception:
            v = None
    u = (unit_txt or "").strip()
    return v, u or None

def parse_price_xml(xml_bytes: bytes):
    items_out = []
    root = _xml_root_robust(xml_bytes)

    item_nodes = []
    for name in ("Item", "Product", "ProductItem", "PriceItem"):
        item_nodes = _findall_by_localname(root, name)
        if item_nodes: break
    if not item_nodes:
        for cont in ("Items", "Products"):
            for container in _findall_by_localname(root, cont):
                for name in ("Item", "Product", "ProductItem", "PriceItem"):
                    cand = [n for n in list(container) if _strip_ns(n.tag).lower() == name.lower()]
                    if cand:
                        item_nodes = cand; break
            if item_nodes: break

    name_tags   = ("ItemName","ProductName","Name","ItemDesc","Description")
    price_tags  = ("ItemPrice","Price","UnitPrice","CurrentPrice")
    unit_tags   = ("Unit","UnitOfMeasure")
    qty_tags    = ("UnitQty","Quantity","QuantityInPackage")
    barcode_tags= ("ItemBarcode","Barcode","ItemCode","ItemId","ManufactureItemCode")
    brand_tags  = ("ManufacturerName","Brand")
    cat_tags    = ("Category","CategoryName")
    curr_tags   = ("Currency",)

    for node in item_nodes:
        product = _first_text(node, *name_tags)
        price   = _to_float(_first_text(node, *price_tags))
        unit    = _first_text(node, *unit_tags)
        qty     = _first_text(node, *qty_tags)
        size_value, size_unit = _split_size(qty, unit)

        item = {
            "product": product,
            "price": price,
            "unit": unit or None,
            "barcode": _first_text(node, *barcode_tags) or None,
            "brand": _first_text(node, *brand_tags) or None,
            "category": _first_text(node, *cat_tags) or None,
            "size_value": size_value,
            "size_unit": size_unit,
            "currency": _first_text(node, *curr_tags) or None,
            "in_stock": True if product or price is not None else None,
        }

        if item["product"] or item["price"] is not None:
            items_out.append(item)

    return items_out

def parse_promo_xml(xml_bytes: bytes):
    items_out = []
    root = _xml_root_robust(xml_bytes)

    promo_nodes = []
    for name in ("Promotion","Promo","Deal"):
        promo_nodes = _findall_by_localname(root, name)
        if promo_nodes: break
    if not promo_nodes:
        for cont in ("Promotions","Deals","Promos"):
            for container in _findall_by_localname(root, cont):
                cand = [n for n in list(container) if _strip_ns(n.tag).lower() in ("promotion","promo","deal")]
                if cand:
                    promo_nodes = cand; break
            if promo_nodes: break

    desc_tags   = ("PromotionDescription","Description","Name","Title")
    price_tags  = ("Price","PromoPrice","BenefitPrice","ItemPrice")
    unit_tags   = ("Unit","UnitOfMeasure")
    qty_tags    = ("UnitQty","Quantity","QuantityInPackage")
    barcode_tags= ("ItemBarcode","Barcode","ItemCode","ItemId","ManufactureItemCode")
    brand_tags  = ("ManufacturerName","Brand")
    cat_tags    = ("Category","CategoryName")
    curr_tags   = ("Currency",)
    promo_text_tags = ("PromotionDescription","Description","Title","Name","BenefitDescription")

    for node in promo_nodes:
        product = _first_text(node, *desc_tags)
        unit    = _first_text(node, *unit_tags)
        qty     = _first_text(node, *qty_tags)
        size_value, size_unit = _split_size(qty, unit)

        item = {
            "product": product,
            "price": _to_float(_first_text(node, *price_tags)),
            "unit": unit or None,
            "barcode": _first_text(node, *barcode_tags) or None,
            "brand": _first_text(node, *brand_tags) or None,
            "category": _first_text(node, *cat_tags) or None,
            "size_value": size_value,
            "size_unit": size_unit,
            "currency": _first_text(node, *curr_tags) or None,
            "promo_price": _to_float(_first_text(node, *price_tags)),
            "promo_text": _first_text(node, *promo_text_tags) or None,
            "in_stock": True,
        }

        if item["product"] or item["price"] is not None:
            items_out.append(item)

    return items_out

def looks_like_xml(b: bytes) -> bool:
    head = b[:200].lstrip()
    return head.startswith(b'<?xml') or head.startswith(b'<')
def parse_content(path: str, raw_bytes: bytes):
    buf = decompress_gz_to_bytes(path, raw_bytes)

    lower = path.lower()
    if lower.endswith(".xml") or lower.endswith(".gz") or looks_like_xml_bytes(buf):
        provider, branch, ts = guess_from_path(path)
        items = parse_price_xml(buf, provider=provider, branch=branch, timestamp=ts)
        return {"type": "xml", "items": items}

    text = _decode_bytes_lossless(buf)

    if looks_like_json(text):
        return {"type": "json", "items": parse_price_json(text)}
    elif looks_like_csv(text):
        return {"type": "csv", "items": parse_price_csv(text)}
    elif looks_like_tsv(text):
        return {"type": "tsv", "items": parse_price_tsv(text)}
    else:
        return {"type": "txt", "items": parse_price_txt(text)}


def decompress_gz_to_bytes(obj_body: bytes) -> bytes:
    with gzip.GzipFile(fileobj=io.BytesIO(obj_body)) as gz:
        return gz.read()

def sniff_format(buf: bytes) -> str:
    head = buf[:1024].lstrip()
    if head.startswith(b"{") or head.startswith(b"["):
        return "json"
    if head.startswith(b"<"):
        return "xml"
    text = _decode_smart(buf)
    if "," in text and "\n" in text:
        return "csv"
    if "\t" in text:
        return "tsv"
    return "txt"

def parse_content_generic(buf: bytes):
    fmt = sniff_format(buf)
    if fmt == "json":
        text = _decode_smart(buf)
        data = json.loads(text)
        if isinstance(data, dict) and "items" in data:
            rows = data["items"]
        elif isinstance(data, list):
            rows = data
        else:
            rows = [data]
        return fmt, rows
    elif fmt == "xml":
        root = _xml_root_robust(buf)
        rows = []
        for item in root.findall(".//Item"):
            rows.append({
                "product": _text(item.find(".//ItemName")) or _text(item.find(".//Name")) or "",
                "price": _text(item.find(".//ItemPrice")) or _text(item.find(".//Price")) or "",
                "unit": _text(item.find(".//UnitQty")) or _text(item.find(".//Unit")) or "",
            })
        return fmt, rows
    elif fmt in ("csv", "tsv"):
        delimiter = "," if fmt == "csv" else "\t"
        text = _decode_smart(buf)
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        return fmt, list(reader)
    else:
        text = _decode_smart(buf)
        return fmt, [line for line in text.splitlines() if line.strip()]

_ws_re = re.compile(r"\s+", re.UNICODE)

def _clean_text(s):
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', s)
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
            ptxt = ptxt.replace("₪", "").replace("ש\"ח", "").replace("שח", "")
            ptxt = ptxt.replace(",", "").replace(" ", "").strip()
            try:
                price = float(ptxt) if ptxt else None
            except Exception:
                price = None

        if name or price is not None:
            out.append({
                "product": name,
                "price": price,
                "unit": unit or ""
            })
    return out