# parser.py
import csv
import io
import json
from typing import Any, Dict, Iterable, List, Optional

def parse_to_items(text: str) -> List[Dict[str, Any]]:
    s = text.lstrip()
    if s.startswith("<"):
        return parse_xml(s)
    if s.startswith("{") or s.startswith("["):
        return parse_json(s)
    return parse_csv(s)

# ---------- XML ----------
def parse_xml(xml_text: str) -> List[Dict[str, Any]]:
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml_text)
    items: List[Dict[str, Any]] = []
    candidates = [".//Items/Item", ".//Item", ".//Products/Product"]
    nodes = []
    for path in candidates:
        nodes = root.findall(path)
        if nodes: break
    for n in nodes:
        item = {
            "product": text_of(n, ["ItemName", "ProductName", "Name", "שם_מוצר"]),
            "price": to_float(text_of(n, ["ItemPrice", "Price", "מחיר"])),
            "unit":  text_of(n, ["Unit", "UnitQty", "Quantity", "יחידה"]),
        }
        if any(v is not None for v in item.values()):
            items.append(item)
    return items

def text_of(node, tags: Iterable[str]) -> Optional[str]:
    for t in tags:
        child = node.find(t)
        if child is not None and child.text:
            v = child.text.strip()
            if v: return v
    return None

# ---------- JSON ----------
def parse_json(js_text: str) -> List[Dict[str, Any]]:
    data = json.loads(js_text)
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        data = data["items"]
    if not isinstance(data, list):
        data = [data]
    out: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict): continue
        out.append({
            "product": first_nonempty(row.get("product"), row.get("name"), row.get("ItemName"), row.get("ProductName"), row.get("שם_מוצר")),
            "price":  to_float(first_nonempty(row.get("price"), row.get("ItemPrice"), row.get("Price"), row.get("מחיר"))),
            "unit":   first_nonempty(row.get("unit"), row.get("Unit"), row.get("UnitQty"), row.get("Quantity"), row.get("יחידה")),
        })
    return out

# ---------- CSV ----------
def parse_csv(text: str) -> List[Dict[str, Any]]:
    sample = "\n".join(text.splitlines()[:10]) + "\n"
    try:
        dialect = csv.Sniffer().sniff(sample)
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    out: List[Dict[str, Any]] = []
    for row in reader:
        if not row: continue
        norm = {(k or "").strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        product = first_nonempty(norm.get("product"), norm.get("name"), norm.get("itemname"), norm.get("productname"), norm.get("שם_מוצר"))
        price   = to_float(first_nonempty(norm.get("price"), norm.get("itemprice"), norm.get("מחיר")))
        unit    = first_nonempty(norm.get("unit"), norm.get("unitqty"), norm.get("quantity"), norm.get("יחידה"))
        if product or price is not None or unit:
            out.append({"product": product, "price": price, "unit": unit})
    return out

# ---------- helpers ----------
def first_nonempty(*vals):
    for v in vals:
        if isinstance(v, str):
            s = v.strip()
            if s: return s
        elif v is not None:
            return v
    return None

def to_float(val) -> Optional[float]:
    if val is None: return None
    if isinstance(val, (int, float)):
        try: return float(val)
        except Exception: return None
    s = str(val).strip()
    s = s.replace("\u200f", "").replace("\xa0", " ").strip()  # RTL mark & NBSP
    s = s.replace("₪", "").replace("ILS", "").strip()
    if "," in s and "." not in s:
        s = s.replace(".", "").replace(",", ".")  # decimal comma
    else:
        s = s.replace(",", "")                    # thousands comma
    s = s.replace(" ", "")
    try: return float(s)
    except Exception: return None
