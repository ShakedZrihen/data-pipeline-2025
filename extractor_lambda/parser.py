import gzip, io, json, csv
from xml.etree import ElementTree as ET
import re
import logging
import xml.etree.ElementTree as ET
logger = logging.getLogger("extractor")
PREFERRED_DECODINGS = ("utf-8-sig", "utf-8", "windows-1255", "cp1255", "iso-8859-8")

# ------------  XML & Hebrew encoding fixes ---------------

def _decode_smart(b: bytes) -> str:
    """
    מפענח bytes לעברית בצורה עמידה:
    מתחיל עם UTF-8 (עם/בלי BOM), Windows-1255, ISO-8859-8, ואז CP1255.
    """
    # הסרת BOM אם קיים
    if b.startswith(b'\xef\xbb\xbf'):
        b = b[3:]
    elif b.startswith((b'\xff\xfe', b'\xfe\xff')):
        # UTF-16 BOM
        try:
            return b.decode('utf-16')
        except UnicodeDecodeError:
            pass
    
    # רשימת קידודים לנסות לפי סדר עדיפות
    encodings = [
        'utf-8',
        'windows-1255',  # הקידוד הנפוץ ביותר לעברית בישראל
        'iso-8859-8',    # עברית ישנה יותר
        'cp1255',        # דומה ל-windows-1255
        'utf-16',
        'latin1'         # כמעט תמיד עובד אבל עלול להיות לא נכון
    ]
    
    for enc in encodings:
        try:
            decoded = b.decode(enc)
            # בדיקה פשוטה שהטקסט נראה תקין (יש בו תווים עבריים או אנגליים)
            if any(ord(c) >= 0x0590 and ord(c) <= 0x05FF for c in decoded[:1000]) or \
               any(c.isascii() and c.isalnum() for c in decoded[:1000]):
                return decoded
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    # מקרה קצה: נעדיף לא לאבד את כל הקובץ
    logger.warning("Failed to decode with all Hebrew encodings, using UTF-8 with replacement")
    return b.decode("utf-8", errors="replace")
def _decode_bytes_lossless(b: bytes) -> str:
    """
    נסה כמה קידודים בעברית וב-UTF-8 ב-STRICT (ללא החלפות),
    אם כולם נכשלים ניפול ל-latin-1 (לא תזרוק שגיאה ושומרת את כל הבייטים).
    """
    last_err = None
    for enc in PREFERRED_DECODINGS:
        try:
            return b.decode(enc)  # strict
        except UnicodeDecodeError as e:
            last_err = e
            continue
    # fallback אחרון – לא ישבור תווים, פשוט ימפה בייט לכל קוד-נקודה
    return b.decode("latin-1")
def _has_xml_decl(bs: bytes) -> bool:
    head = bs[:200].lstrip().lower()
    return head.startswith(b'<?xml')
# --- החלפה מלאה של _xml_root_robust ---
def _xml_root_robust(xml_bytes: bytes):
    """
    פרסור XML עמיד לקידודים (UTF-8/Windows-1255/ISO-8859-8 ...).
    לא משתמשים ב-errors='replace' כדי לא להפוך אותיות ל־�.
    """
    # קודם כל - אם יש הכרזה על קידוד בכותרת XML, נשתמש בה ב-strict
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
            text = xml_bytes.decode(enc_decl)  # strict
            return ET.fromstring(text)
        except UnicodeDecodeError:
            pass  # נמשיך לניסיונות הבאים

    # נסה רשימת קידודים מועדים
    for enc in PREFERRED_DECODINGS:
        try:
            text = xml_bytes.decode(enc)  # strict
            return ET.fromstring(text)
        except UnicodeDecodeError:
            continue

    # ניסיון סופי – אם המחרוזת כבר UTF-8 חוקי כ-bytes
    try:
        return ET.fromstring(xml_bytes)
    except Exception:
        # fallback אחרון: latin-1 -> string
        text = xml_bytes.decode("latin-1")
        return ET.fromstring(text)


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def _text(el):
    """חילוץ טקסט עם ניקוי רווחים מיותרים"""
    if el is None:
        return ""
    text = el.text or ""
    # ניקוי רווחים ותווי בקרה
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
# --- עזר קטן: מריחים XML מהבייטים (להוסיף אם אין) ---
def looks_like_xml_bytes(b: bytes) -> bool:
    head = b[:200].lstrip()
    return head.startswith(b'<?xml') or b'<Items' in head or b'<root' in head or b'<item' in head

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
            # ניקוי מחיר - כל הסימנים הנפוצים של שקל
            pprice_txt = pprice_txt.replace("₪", "").replace("ש\"ח", "").replace("שח", "")
            pprice_txt = pprice_txt.replace(",", "").replace(" ", "").strip()
            try:
                pprice = float(pprice_txt)
            except Exception:
                pprice = None

        if pname or pprice is not None:
            items_out.append({
                "product": pname, 
                "price": pprice, 
                "unit": punit
            })
    
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
            # ניקוי מחיר
            price_txt = price_txt.replace("₪", "").replace("ש\"ח", "").replace("שח", "")
            price_txt = price_txt.replace(",", "").replace(" ", "").strip()
            try:
                price = float(price_txt)
            except Exception:
                price = None

        if desc or price is not None:
            items_out.append({
                "product": desc, 
                "price": price, 
                "unit": unit
            })
    
    return items_out

def looks_like_xml(b: bytes) -> bool:
    head = b[:200].lstrip()
    return head.startswith(b'<?xml') or head.startswith(b'<')
# --- עדכון בתוך parse_content (רק החלק של sniff/decoding) ---
def parse_content(path: str, raw_bytes: bytes):
    buf = decompress_gz_to_bytes(path, raw_bytes)

    # מריחים פורמט על בסיס BYTES, לא מפענחים מוקדם מדי
    lower = path.lower()
    if lower.endswith(".xml") or lower.endswith(".gz") or looks_like_xml_bytes(buf):
        # XML: נפרסר ישירות מהבייטים עם _xml_root_robust
        provider, branch, ts = guess_from_path(path)
        items = parse_price_xml(buf, provider=provider, branch=branch, timestamp=ts)
        return {"type": "xml", "items": items}

    # טקסט לא-XML (csv/tsv/txt/json) – מפענחים דרך פונקציה lossless
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
    """פרסור כללי לתבניות שונות"""
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

# ביטוי רגולרי לניקוי רווחים מיותרים
_ws_re = re.compile(r"\s+", re.UNICODE)

def _clean_text(s):
    """ניקוי טקסט מרווחים מיותרים ותווי בקרה"""
    if s is None:
        return ""
    s = str(s)
    # הסרת תווי בקרה והחלפת רווחים מרובים ברווח יחיד
    s = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', '', s)
    s = _ws_re.sub(" ", s).strip()
    return s

def normalize_rows(rows):
    """נרמול שורות עם טיפול נכון בעברית"""
    out = []
    for r in rows:
        name = _clean_text(r.get("product"))
        unit = _clean_text(r.get("unit"))
        price = r.get("price")

        if price is None or price == "":
            ptxt = _clean_text(r.get("price"))
            # ניקוי כל סוגי הסימונים של שקל
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