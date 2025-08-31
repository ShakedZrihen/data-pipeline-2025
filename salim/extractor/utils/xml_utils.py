import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# TODO: need to correct all of this ! . 

def _strip_ns(tag: str) -> str:
    # remove XML namespace like {urn:...}Tag -> Tag
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag

def parse_xml_items(xml_text: str):
    """
    Try to find repeating "item" elements and map child tags to:
    product, price, unit (student-level heuristics).
    Returns: list[{"product": str|None, "price": float|None, "unit": str|None}]
    """
    items = []
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        print("XML parse error:", e)
        return items

    # common item-like element names
    ITEM_TAGS = {"Item", "item", "Product", "product", "Row", "row"}

    # child names we care about
    NAME_TAGS  = {"ItemName", "Name", "Description", "ProdName", "ProductName", "שם"}
    PRICE_TAGS = {"ItemPrice", "Price", "UnitPrice", "CurrentPrice", "מחיר"}
    UNIT_TAGS  = {"Unit", "UnitOfMeasure", "UOM", "יחידה"}

    def first_child_text(elem, wanted):
        # look only one level down; if not found, scan deeper
        for ch in list(elem):
            if _strip_ns(ch.tag) in wanted and (ch.text or "").strip():
                return ch.text.strip()
        for ch in elem.iter():
            if ch is elem:
                continue
            if _strip_ns(ch.tag) in wanted and (ch.text or "").strip():
                return ch.text.strip()
        return None

    for node in root.iter():
        if _strip_ns(node.tag) in ITEM_TAGS:
            name  = first_child_text(node, NAME_TAGS)
            price = first_child_text(node, PRICE_TAGS)
            unit  = first_child_text(node, UNIT_TAGS)

            fprice = None
            if price:
                s = price.replace("₪", "").replace(",", " ").strip()
                try:
                    # try first token as float if needed
                    fprice = float(s.split()[0])
                except Exception:
                    fprice = None

            if name or fprice is not None:
                items.append({"product": name, "price": fprice, "unit": unit})

    return items

def iso_from_filename(fname: str):
    """
    Convert price_YYYYMMDD_HHMMSS.gz to ISO time in Z.
    If not found -> return current UTC.
    """
    m = re.search(r"(\d{8}_\d{6})", fname)
    if not m:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        dt = datetime.strptime(m.group(1), "%Y%m%d_%H%M%S").replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
