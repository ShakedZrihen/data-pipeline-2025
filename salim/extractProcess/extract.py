import re, io, datetime, xml.etree.ElementTree as ET
from .s3io import download_gz

KEY_RE = re.compile(
    r'^providers/[^/]+/[^/]+/(?P<type>pricesFull|promoFull)_(?P<ts>\d{12,14})\.gz$',
    re.IGNORECASE
)

def _iso_from_ts(ts_str: str):
    if not ts_str:
        return datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
    fmt = '%Y%m%d%H%M' if len(ts_str) == 12 else '%Y%m%d%H%M%S'
    dt = datetime.datetime.strptime(ts_str, fmt).replace(tzinfo=datetime.timezone.utc)
    return dt.isoformat().replace('+00:00', 'Z')

def _clean(txt):
    return str(txt).strip() if txt else None

def _to_float(txt):
    if txt is None:
        return None
    try:
        return float(str(txt).replace(',', '.').strip())
    except Exception:
        return None


def parse_pricefull(xml_stream: io.BytesIO):

    provider, branch, items = None, None, []

    def _fmt_amount_str(s: str | None) -> str | None:
        s = _clean(s)
        if not s:
            return None
        try:
            f = float(s.replace(",", ""))
            s2 = ("%.10g" % f)
            if "." in s2:
                s2 = s2.rstrip("0").rstrip(".")
            return s2
        except Exception:
            return s

    for elem in ET.iterparse(xml_stream, events=("end",)):
        tag = elem.tag.lower()

        if tag == "chainid":
            provider = provider or _clean(elem.text)
        elif tag == "storeid":
            branch = branch or _clean(elem.text)

        elif tag == "item":
            code   = _clean(elem.findtext("ItemCode"))
            name   = _clean(elem.findtext("ItemName"))
            manu_name = _clean(elem.findtext("ManufacturerName"))
            manu_desc = _clean(elem.findtext("ManufacturerItemDescription"))
            price  = _to_float(elem.findtext("ItemPrice"))
            qty_str   = _fmt_amount_str(elem.findtext("Quantity"))
            unit_qty  = _clean(elem.findtext("UnitQty")) 
            uom_fallback = _clean(elem.findtext("UnitOfMeasure"))

            # because we saw that "Quantity" and "UnitQty" together are best for "unit"
            unit = None
            if qty_str and unit_qty:
                unit = f"{qty_str} {unit_qty}"
            elif qty_str:
                unit = qty_str
            elif unit_qty:
                unit = unit_qty
            else:
                unit = uom_fallback

            if name and price is not None:
                item = {
                    "productId": code,
                    "product": name,
                    "price": price, 
                    "unit": unit or "",
                    "manu_name": manu_name,
                    "manu_desc": manu_desc
                }
                items.append(item)

            elem.clear()

    return provider, branch, items


def parse_promofull(xml_stream: io.BytesIO):
    
    provider, branch = None, None
    items = []

    current = None

    for event, elem in ET.iterparse(xml_stream, events=("start", "end")):
        tag = elem.tag
        lt = tag.lower()

        if event == "end":
            if lt == "chainid":
                provider = provider or _clean(elem.text)
            elif lt == "storeid":
                branch = branch or _clean(elem.text)

        if tag == "Promotion" and event == "start":
            current = {
                "description": None,
                "min_qty": None,
                "discounted_price": None,
                "item_codes": [],
            }

        elif tag == "Promotion" and event == "end":
            if current:
                desc = current["description"]
                price = current["discounted_price"]
                min_qty = current["min_qty"]
                codes = current["item_codes"] or []

                if desc is not None and price is not None and min_qty is not None and codes:
                    items.append({
                        "productId": codes,
                        "product": desc,
                        "price": price,
                        "unit": min_qty,
                    })

            current = None
            elem.clear()

        elif current is not None and event == "end":
            if lt == "promotiondescription":
                current["description"] = _clean(elem.text)
            elif lt == "minqty":
                current["min_qty"] = _to_float(elem.text)
            elif lt == "discountedprice":
                current["discounted_price"] = _to_float(elem.text)
            elif lt == "itemcode":
                code = _clean(elem.text)
                if code:
                    current["item_codes"].append(code)

            elem.clear()

    return provider, branch, items


def process_s3_object_to_json(bucket: str, key: str):
    m = KEY_RE.match(key)
    if not m:
        print(f"Skipping {key} – path not matching")
        return None
    data_type = m.group('type')
    ts = m.group('ts')
    timestamp = _iso_from_ts(ts)
    xml_stream = download_gz(bucket, key)

    if data_type.lower() == "pricesfull":
        provider, branch, items = parse_pricefull(xml_stream)
    else:
        provider, branch, items = parse_promofull(xml_stream)

    return {
        "provider": provider,
        "branch": branch,
        "type": data_type,
        "timestamp": timestamp,
        "items": items
    }
