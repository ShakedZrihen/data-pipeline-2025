import os, json, io, gzip, zipfile, tempfile, logging, sys, traceback
from pathlib import Path
from datetime import datetime
import boto3
from botocore.config import Config
import xml.etree.ElementTree as ET
import re
from typing import Optional, Tuple
from utils.logging_config import setup_logging
from parser import decompress_gz_to_bytes
from normalizer import parse_key
from producer import send_message
from db import upsert_last_run
from config import S3_SIMULATOR_ROOT

# -----------------------------------------------------------------------------
# logging
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.getLogger().handlers = [handler]
logging.getLogger().setLevel(logging.INFO)
log = setup_logging()
logger = logging.getLogger("extractor")

# -----------------------------------------------------------------------------
# AWS
def _s3_client():
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    region = os.getenv("AWS_REGION", "us-east-1")
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client("s3", region_name=region, endpoint_url=endpoint, config=cfg)

def _queue_enabled() -> bool:
    return os.getenv("ENABLE_QUEUE") == "1" and bool(os.getenv("OUTPUT_QUEUE_URL"))

# -----------------------------------------------------------------------------
# XML helpers (hebrew-safe)
def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def _decode_smart(b: bytes) -> str:
    if b.startswith(b'\xef\xbb\xbf'):
        b = b[3:]
    for enc in ('utf-8', 'utf-16', 'windows-1255', 'cp1255', 'iso-8859-8'):
        try:
            s = b.decode(enc)
            if s.count('�') < 3:
                return s
        except UnicodeDecodeError:
            continue
    return b.decode('utf-8', errors='replace')

def _xml_root_robust(xml_data):
    try:
        return ET.fromstring(xml_data if isinstance(xml_data, (bytes, bytearray)) else xml_data.encode("utf-8"))
    except ET.ParseError:
        txt = _decode_smart(xml_data) if isinstance(xml_data, (bytes, bytearray)) else xml_data
        return ET.fromstring(txt)
BARCODE_TAGS = ("ItemCode", "ItemId", "Code", "Barcode", "ManufacturerItemId", "ExternalCode")

def _first_text(node, candidates):
    for c in candidates:
        for child in list(node):
            if _strip_ns(child.tag).lower() == c.lower():
                t = _text(child)
                if t:
                    return t
    # גם אטריביוטים (יש קבצים ששמים barcode בתור <Item Code="...">)
    for c in candidates:
        v = node.attrib.get(c) or node.attrib.get(c.lower())
        if v:
            v = re.sub(r"\s+", " ", v).strip()
            if v:
                return v
    return ""

BARCODE_RE = re.compile(r"\b\d{8,14}\b")  # fallback אם אין טאג מפורש
def _fallback_barcode_from_name(name: str) -> str | None:
    if not name:
        return None
    m = BARCODE_RE.search(name)
    return m.group(0) if m else None
def _findall_by_localname(root, name):
    lname = name.lower()
    out = []
    for el in root.iter():
        if _strip_ns(el.tag).lower() == lname:
            out.append(el)
    return out

def _first_text(node, candidates):
    for c in candidates:
        for child in list(node):
            if _strip_ns(child.tag).lower() == c.lower():
                t = (child.text or "").strip()
                if t:
                    return " ".join(t.split())
    return ""

def _normalize_xml_candidate(b: bytes) -> bytes:
    # drop BOM / decode UTF-16 if needed, but return bytes for ET
    head = b[:6]
    if head.startswith(b"\xff\xfe") or head.startswith(b"\xfe\xff") or b"\x00" in head:
        return _decode_smart(b).encode("utf-8")
    if head.startswith(b"\xef\xbb\xbf"):
        return b[3:]
    return b

def _candidates_from_payload(raw: bytes):
    # supports single XML, ZIP of XMLs, or *.xml.gz inside ZIP
    out = []
    if len(raw) >= 4 and raw[:4] == b"PK\x03\x04":
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                for info in z.infolist():
                    name = info.filename.lower()
                    data = z.read(info)
                    if name.endswith(".gz"):
                        try:
                            data = gzip.decompress(data)
                            name = name[:-3]
                        except Exception:
                            pass
                    if name.endswith(".xml"):
                        out.append(_normalize_xml_candidate(data))
        except Exception:
            out.append(raw)
    else:
        out.append(_normalize_xml_candidate(raw))
    return out

def _parse_price_xml(xml_bytes):
    items = []
    root = _xml_root_robust(xml_bytes)
    item_nodes = []
    for n in ("Item", "Product", "ProductItem", "PriceItem"):
        item_nodes = _findall_by_localname(root, n)
        if item_nodes:
            break
    if not item_nodes:
        for cont in ("Items", "Products"):
            for container in _findall_by_localname(root, cont):
                for n in ("Item", "Product", "ProductItem", "PriceItem"):
                    cand = [x for x in list(container) if _strip_ns(x.tag).lower() == n.lower()]
                    if cand:
                        item_nodes = cand
                        break
            if item_nodes:
                break
    name_tags  = ("ItemName", "ProductName", "Name", "ItemDesc", "Description")
    price_tags = ("ItemPrice", "Price", "UnitPrice", "CurrentPrice")
    unit_tags  = ("Unit", "UnitOfMeasure", "UnitQty", "Quantity", "QuantityInPackage")
    barcode_tags = ("ItemCode", "Barcode", "Code", "ItemId", "ManufacturerItemCode")

    for node in item_nodes:
        pname = _first_text(node, name_tags)
        punit = _first_text(node, unit_tags)
        pprice_txt = _first_text(node, price_tags)
        pbarcode   =_first_text(node, barcode_tags)

        price = None
        if pprice_txt:
            pprice_txt = (pprice_txt.replace("₪","")
                                      .replace("ש\"ח","")
                                      .replace("שח","")
                                      .replace(",","")
                                      .replace(" ",""))
            try:
                price = float(pprice_txt)
            except Exception:
                price = None
        if pname or price is not None:
            items.append({"product": pname, "price": price, "unit": punit  ,"barcode": pbarcode or None,})
    return items

def _parse_promo_xml(xml_bytes):
    items = []
    root = _xml_root_robust(xml_bytes)
    promo_nodes = []
    for n in ("Promotion", "Promo", "Deal"):
        promo_nodes = _findall_by_localname(root, n)
        if promo_nodes:
            break
    if not promo_nodes:
        for cont in ("Promotions", "Deals", "Promos"):
            for container in _findall_by_localname(root, cont):
                for n in ("Promotion", "Promo", "Deal"):
                    cand = [x for x in list(container) if _strip_ns(x.tag).lower() == n.lower()]
                    if cand:
                        promo_nodes = cand
                        break
            if promo_nodes:
                break
    desc_tags  = ("PromotionDescription", "Description", "Name", "Title")
    price_tags = ("Price", "PromoPrice", "BenefitPrice", "ItemPrice")
    unit_tags  = ("Unit", "UnitOfMeasure")

    for node in promo_nodes:
        desc = _first_text(node, desc_tags)
        price_txt = _first_text(node, price_tags)
        unit = _first_text(node, unit_tags)
        pcode  = _first_text(node, BARCODE_TAGS) or _fallback_barcode_from_name(desc)

        price = None
        if price_txt:
            price_txt = (price_txt.replace("₪","")
                                   .replace("ש\"ח","")
                                   .replace("שח","")
                                   .replace(",","")
                                   .replace(" ",""))
            try:
                price = float(price_txt)
            except Exception:
                price = None
        if desc or price is not None:
            items.append({"product": desc, "price": price, "unit": unit,"barcode": pcode})
    return items

def extract_items_from_bytes(raw: bytes, kind: str):
    any_err = None
    for cand in _candidates_from_payload(raw):
        try:
            return _parse_promo_xml(cand) if kind.lower() == "promofull" else _parse_price_xml(cand)
        except Exception as e:
            any_err = e
            continue
    if any_err:
        logger.warning("XML parse failed: %s", any_err)
    else:
        logger.warning("No items extracted from XML.")
    return []
SIZE_PAT = re.compile(r'(?P<val>\d+(?:[.,]\d+)?)\s*(?P<unit>מ"ל|ליטר|גרם|קג|ק"ג|קילוגרם|שקית|יחידות?|pcs|g|kg|ml|l)\b', re.IGNORECASE)

UNIT_NORMALIZE = {
    "ק\"ג": "קג", "קג": "קג", "קילוגרם": "קג", "kg": "קג",
    "גרם": "גרם", "g": "גרם",
    "מ\"ל": "מ\"ל", "ml": "מ\"ל",
    "ליטר": "ליטר", "l": "ליטר",
}

def _parse_size_from_name(name: str) -> Tuple[Optional[float], Optional[str]]:
    if not name:
        return None, None
    m = SIZE_PAT.search(name)
    if not m:
        return None, None
    try:
        val = float(m.group("val").replace(",", "."))
    except:
        val = None
    unit = m.group("unit")
    unit = UNIT_NORMALIZE.get(unit, unit)
    return val, unit

def _brand_from_name(name: str) -> Optional[str]:
    if not name:
        return None
    known = ("תנובה", "טרה", "יוטבתה", "שטראוס", "YOLO", "מן", "עלית", "נספרסו")
    for k in known:
        if k in name:
            return k
    return None

def _category_guess(name: str) -> Optional[str]:
    if not name:
        return None
    kw = {
        "חלב": "חלב ומוצריו",
        "יוגורט": "חלב ומוצריו",
        "מעדן": "חלב ומוצריו",
        "גבינה": "חלב ומוצריו",
        "קוטג": "חלב ומוצריו",
        "במבה": "חטיפים",
        "וופל": "חטיפים",
        "סוכר": "מכולת יבשה",
        "אורז": "מכולת יבשה",
        "קמח": "מכולת יבשה",
    }
    for k, v in kw.items():
        if k in name:
            return v
    return None

def normalize_item_to_product(item: dict, *, provider: str, branch: str, iso_ts: str, is_promo: bool) -> dict:
    name = (item.get("product") or "").strip()
    price = item.get("price")
    unit  = item.get("unit")
    barcode = (item.get("barcode") or _fallback_barcode_from_name(name))
    size_value, size_unit = _parse_size_from_name(name)
    brand = _brand_from_name(name)
    category = _category_guess(name)

    product = {
        "barcode": barcode,
        "product": name or None,
        "brand": brand,
        "category": category,
        "size_value": size_value,
        "size_unit": size_unit or unit,
        "price": price,
        "currency": "ILS",
        "promo_price": price if is_promo else None,
        "promo_text": name if is_promo else None,
        "in_stock": True if price is not None else False,
        "collected_at": iso_ts,
    }
    return product
# -----------------------------------------------------------------------------
def _save_json(path: str, data: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _outbox_filename(provider: str, branch: str, type_: str, iso_ts: str) -> str:
    safe_ts = iso_ts.replace(":", "").replace("-", "").replace("T", "_").replace("Z", "")
    return f"{provider}_{branch}_{type_}_{safe_ts}.json"

# -----------------------------------------------------------------------------
# S3 -> outbox (+ optional SQS)
def lambda_handler(event, context=None):
    log.info("Received event")
    records = event.get("Records", [])
    if not records:
        return {"ok": True, "processed": 0}

    s3 = _s3_client()
    processed = errors = 0

    for rec in records:
        try:
            bucket = rec["s3"]["bucket"]["name"]
            key    = rec["s3"]["object"]["key"]
            if not key.lower().endswith(".gz"):
                log.info("Skip non-gz: %s", key)
                continue

            provider, branch, type_, iso_ts = parse_key(key)

            # fetch + ungzip
            obj = s3.get_object(Bucket=bucket, Key=key)
            gz_bytes = obj["Body"].read()
            xml_bytes = decompress_gz_to_bytes(gz_bytes)

            # extract items
            items = extract_items_from_bytes(xml_bytes, type_)
            items_total = len(items)
            items_sample = items[: min(items_total, 20)]

            tmp_dir = (os.getenv("LAMBDA_TMP") or os.getenv("TMPDIR")
                       or ("/tmp" if os.name != "nt" else tempfile.gettempdir()))
            outbox_dir = os.path.join(tmp_dir, "outbox")
            outbox_path = os.path.join(outbox_dir, _outbox_filename(provider, branch, type_, iso_ts))

            full_payload = {
                "provider": provider,
                "branch": branch,
                "type": type_,
                "timestamp": iso_ts,
                "items_total": items_total,
                "items": items,
                "s3_bucket": bucket,
                "s3_key": key,
            }
            _save_json(outbox_path, full_payload)
            log.info("Wrote outbox file: %s", outbox_path)

            if _queue_enabled():
                envelope = {
                    "provider": provider,
                    "branch": branch,
                    "type": type_,
                    "timestamp": iso_ts,
                    "items_total": items_total,
                    "items_sample": items_sample,
                    "outbox_path": outbox_path,
                }
                send_message(envelope, outbox_path=outbox_path)

            # bookkeeping
            try:
                upsert_last_run(provider, branch, type_, iso_ts)
            except Exception as e:
                log.warning("last_run upsert failed: %s", e)

            processed += 1

        except Exception as e:
            errors += 1
            log.error("Error: %s\n%s", e, traceback.format_exc())

    return {"ok": errors == 0, "processed": processed, "errors": errors}

# -----------------------------------------------------------------------------
def process_local_file(file_path: str):
    p = Path(file_path).resolve()
    root = Path(S3_SIMULATOR_ROOT).resolve() / "providers"
    rel = p.relative_to(root)
    key = f"providers/{str(rel).replace(os.sep, '/')}"
    provider, branch, type_, iso_ts = parse_key(key)

    with open(p, "rb") as f:
        gz_bytes = f.read()
    xml_bytes = decompress_gz_to_bytes(gz_bytes)

    items = extract_items_from_bytes(xml_bytes, type_)
    items_total = len(items)
    items_sample = items[: min(items_total, 20)]

    out_dir = Path(__file__).parent / "outbox"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / _outbox_filename(provider, branch, type_, iso_ts)
    is_promo = (type_.lower() == "promofull")
    norm_items = [
        normalize_item_to_product(it, provider=provider, branch=branch, iso_ts=iso_ts, is_promo=is_promo)
        for it in (items or [])
    ]
    full_payload = {
        "provider": provider,
        "branch": branch,
        "type": type_,
        "timestamp": iso_ts,
        "items_total": items_total,
        "items": items,
        "s3_bucket": "local-sim",
        "s3_key": key,
        "supermarket": {
            "name": provider,
            "branch_name": None,
            "city": None,
            "address": None,
            "website": None,
            "created_at": iso_ts,
        },
        "items": norm_items,
        "items_total": len(norm_items),
    }
    _save_json(out_path.as_posix(), full_payload)
    log.info("[local] saved: %s", out_path)

    if _queue_enabled():
        envelope = {
            "provider": provider,
            "branch": branch,
            "type": type_,
            "timestamp": iso_ts,
            "items_total": items_total,
            "items_sample": items_sample,
            "outbox_path": out_path.as_posix(),
        }
        send_message(envelope, outbox_path=out_path.as_posix())

    try:
        upsert_last_run(provider, branch, type_, iso_ts)
    except Exception as e:
        log.warning("[local] last_run upsert failed: %s", e)

    return full_payload

# -----------------------------------------------------------------------------
def _print_preview(items, n=5):
    for it in items[:n]:
        log.info("  - %s | price=%s | unit=%s", it.get("product"), it.get("price"), it.get("unit"))
    if len(items) > n:
        log.info("  ... and %d more", len(items) - n)

def handler(event, context=None):
    recs = event.get("Records", [])
    for rec in recs:
        body = rec.get("body", "{}")
        try:
            msg = json.loads(body)
        except Exception:
            log.warning("Non-JSON body: %s", body[:200]); continue

        provider = msg.get("provider")
        branch   = msg.get("branch")
        type_    = msg.get("type")
        ts       = msg.get("timestamp")
        total    = msg.get("items_total")
        outbox   = msg.get("outbox_path")
        items    = msg.get("items_sample") or []

        log.info("SQS message: %s/%s %s %s total=%s",
                 provider, branch, type_, ts, total)

        if not items and outbox and os.path.exists(outbox):
            try:
                with open(outbox, "r", encoding="utf-8") as f:
                    full = json.load(f)
                items = full.get("items", [])
                log.info("Loaded items from outbox_path: %d", len(items))
            except Exception as e:
                log.warning("Failed reading outbox_path: %s", e)

        _print_preview(items)
    return {"ok": True, "processed": len(recs)}
