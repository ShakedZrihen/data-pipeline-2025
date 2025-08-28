import gzip, io, json, csv
from xml.etree import ElementTree as ET

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

def normalize_rows(rows):
    norm = []
    for r in rows:
        if isinstance(r, dict):
            name = r.get("product") or r.get("ItemName") or ""
            price = r.get("price") or r.get("ItemPrice")
            unit = r.get("unit") or r.get("UnitQty") or ""
        else:
            name = str(r)
            price = None
            unit = ""
        try:
            price = float(str(price).replace(",", ".")) if price else None
        except:
            price = None
        if name:
            norm.append({"product": name, "price": price, "unit": unit})
    return norm
