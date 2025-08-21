import re, io, datetime, xml.etree.ElementTree as ET
from .s3io import download_gz

KEY_RE = re.compile(
    r'^providers/[^/]+/[^/]+/(?P<type>pricesFull|promoFull)_(?P<ts>\d{12,14})\.gz$',
    re.IGNORECASE
)

def _iso_from_ts(ts_str: str) -> str:
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

def _combine_date_time(date_str, time_str):
    if not date_str:
        return None
    ts = time_str or "00:00:00"
    try:
        dt = datetime.datetime.fromisoformat(f"{date_str.strip()} {ts.strip()}")
    except ValueError:
        try:
            dt = datetime.datetime.fromisoformat(date_str.strip())
        except ValueError:
            return None
    return dt.replace(tzinfo=datetime.timezone.utc).isoformat().replace('+00:00', 'Z')

def parse_pricefull(xml_stream: io.BytesIO):
    provider, branch, items = None, None, []
    for event, elem in ET.iterparse(xml_stream, events=("end",)):
        tag = elem.tag.lower()
        if tag == "chainid":
            provider = provider or _clean(elem.text)
        elif tag == "storeid":
            branch = branch or _clean(elem.text)
        elif tag == "item":
            name = _clean(elem.findtext("ItemName"))
            price = _to_float(elem.findtext("ItemPrice"))
            unit  = _clean(elem.findtext("UnitOfMeasure"))
            if name:
                items.append({"product": name, "price": price, "unit": unit})
            elem.clear()
    return provider, branch, items

def parse_promofull(xml_stream: io.BytesIO):
    provider, branch, promos = None, None, []
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
                "promotion_id": None,
                "description": None,
                "start": None,
                "end": None,
                "min_qty": None,
                "discounted_price": None,
                "item_codes": []
            }
        elif tag == "Promotion" and event == "end":
            if current:
                promos.append(current)
            current = None
            elem.clear()
        elif current is not None and event == "end":
            if lt == "promotionid":
                current["promotion_id"] = int(_clean(elem.text) or 0)
            elif lt == "promotiondescription":
                current["description"] = _clean(elem.text)
            elif lt == "promotionstartdate":
                current["_start_date"] = _clean(elem.text)
            elif lt == "promotionstarthour":
                current["_start_time"] = _clean(elem.text)
            elif lt == "promotionenddate":
                current["_end_date"] = _clean(elem.text)
            elif lt == "promotionendhour":
                current["_end_time"] = _clean(elem.text)
            elif lt == "minqty":
                current["min_qty"] = _to_float(elem.text)
            elif lt == "discountedprice":
                current["discounted_price"] = _to_float(elem.text)
            elif lt == "itemcode":
                code = _clean(elem.text)
                if code:
                    current["item_codes"].append(code)
            if "_start_date" in current and not current.get("start"):
                current["start"] = _combine_date_time(current["_start_date"], current.get("_start_time"))
            if "_end_date" in current and not current.get("end"):
                current["end"] = _combine_date_time(current["_end_date"], current.get("_end_time"))
            elem.clear()
    return provider, branch, promos

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
