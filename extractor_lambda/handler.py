import os
import json
import tempfile
import traceback
import logging
from pathlib import Path
import gzip, zipfile, io

import boto3
from botocore.config import Config

from utils.logging_config import setup_logging
from parser import decompress_gz_to_bytes
from normalizer import parse_key
from producer import send_message
from db import upsert_last_run
from config import S3_SIMULATOR_ROOT

import xml.etree.ElementTree as ET

# ---------- logging ----------
log = setup_logging()
logger = logging.getLogger("extractor")

# ---------- AWS clients ----------
def _s3_client():
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    region = os.getenv("AWS_REGION", "us-east-1")
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client("s3", region_name=region, endpoint_url=endpoint, config=cfg)

def _queue_enabled() -> bool:
    """Only send to queue when explicitly enabled AND we have a queue URL."""
    return os.getenv("ENABLE_QUEUE") == "1" and bool(os.getenv("OUTPUT_QUEUE_URL"))

# ---------- XML helpers ----------
def _candidates_from_payload(raw: bytes):
    """
    מקבל bytes אחרי פירוק ה-GZ (decompress_gz_to_bytes),
    ומחזיר רשימת מועמדים ל-XML:
    - אם זה ZIP -> נחזיר את תוכני ה-XML (או gz פנימי שמכיל XML)
    - אחרת -> נחזיר את raw עצמו
    """
    out = []
    # ZIP header
    if len(raw) >= 4 and raw[:4] == b"PK\x03\x04":
        try:
            with zipfile.ZipFile(io.BytesIO(raw)) as z:
                for info in z.infolist():
                    name_l = info.filename.lower()
                    data = z.read(info)
                    # ZIP יכול להכיל gz פנימי
                    if name_l.endswith(".gz"):
                        try:
                            data = gzip.decompress(data)
                        except Exception:
                            pass
                        name_l = name_l[:-3]  # "…xml.gz" -> "…xml"
                    if name_l.endswith(".xml"):
                        out.append(data)
        except Exception:
            # אם משהו נכשל ב-zip, לפחות נחזיר את המקור לנסיונות אחרים
            out.append(raw)
    else:
        out.append(raw)
    return out

def _normalize_xml_encoding(data: bytes) -> bytes | str:
    """
    אם ה-XML ב-UTF-16 / עם NUL-Bytes – ננסה לפענח ל-utf-16 ולהחזיר str.
    אחרת נחזיר את הבייטים כפי שהם.
    """
    head = data[:6]
    # BOMs נפוצים או NUL-Bytes
    if head.startswith(b"\xff\xfe") or head.startswith(b"\xfe\xff") or b"\x00" in head:
        try:
            return data.decode("utf-16", errors="replace")
        except Exception:
            return data  # ננסה כמו שהוא
    # UTF-8 BOM
    if head.startswith(b"\xef\xbb\xbf"):
        return data[3:]
    return data

def _strip_ns(tag: str) -> str:
    # "{ns}ItemPrice" -> "ItemPrice"
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag

def _text(el):
    return (el.text or "").strip() if el is not None else ""

def _first_text_by_candidates(node, candidates):
    # try children by localname order
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

def parse_price_xml(xml_bytes: bytes):
    """Extract items from PriceFull-like XMLs into [{'product','price','unit'}, ...]."""
    items_out = []
    root = ET.fromstring(xml_bytes)

    item_nodes = []
    for name in ("Item", "Product", "ProductItem", "PriceItem"):
        item_nodes = _findall_by_localname(root, name)
        if item_nodes:
            break
    if not item_nodes:
        # sometimes under <Items> or <Products>
        for cont in ("Items", "Products"):
            for container in _findall_by_localname(root, cont):
                for name in ("Item", "Product", "ProductItem", "PriceItem"):
                    item_nodes = [n for n in list(container) if _strip_ns(n.tag).lower() == name.lower()]
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
            except:
                pprice = None

        if pname or pprice is not None:
            items_out.append({"product": pname, "price": pprice, "unit": punit})
    return items_out

def parse_promo_xml(xml_bytes: bytes):
    """Extract items from PromoFull-like XMLs."""
    items_out = []
    root = ET.fromstring(xml_bytes)

    promo_nodes = []
    for name in ("Promotion", "Promo", "Deal"):
        promo_nodes = _findall_by_localname(root, name)
        if promo_nodes:
            break
    if not promo_nodes:
        for cont in ("Promotions", "Deals", "Promos"):
            for container in _findall_by_localname(root, cont):
                for name in ("Promotion", "Promo", "Deal"):
                    promo_nodes = [n for n in list(container) if _strip_ns(n.tag).lower() == name.lower()]
                    if promo_nodes:
                        break
            if promo_nodes:
                break

    desc_tags  = ("PromotionDescription", "Description", "Name", "Title")
    price_tags = ("Price", "PromoPrice", "BenefitPrice", "ItemPrice")
    unit_tags  = ("Unit", "UnitOfMeasure")

    for node in promo_nodes:
        desc  = _first_text_by_candidates(node, desc_tags)
        price_txt = _first_text_by_candidates(node, price_tags)
        unit  = _first_text_by_candidates(node, unit_tags)

        price = None
        if price_txt:
            price_txt = price_txt.replace("₪", "").replace(",", "").strip()
            try:
                price = float(price_txt)
            except:
                price = None

        if desc or price is not None:
            items_out.append({"product": desc, "price": price, "unit": unit})
    return items_out
def extract_items_from_bytes(raw: bytes, kind: str):
    """
    kind: 'pricesFull' / 'promoFull'
    מפעיל זיהוי ZIP פנימי ו-UTF-16, ומנסה לפרסר את ה-XML.
    מחזיר את ה-items מהראשון שמצליח; אם אף אחד לא מצליח, מחזיר [].
    """
    candidates = _candidates_from_payload(raw)
    any_error = None
    for cand in candidates:
        xml_data = _normalize_xml_encoding(cand)
        try:
            if kind.lower() == "promofull":
                items = parse_promo_xml(xml_data)
            else:
                items = parse_price_xml(xml_data)
            if items:
                return items
            any_error = None
        except Exception as e:
            any_error = e
            continue

    if any_error:
        logger.warning("XML parse failed: %s", any_error)
    else:
        logger.warning("No items extracted from XML candidates.")
    return []


# ---------- Lambda handler ----------
def lambda_handler(event, context=None):
    log.info(f"Received event: {json.dumps(event)[:500]}")
    records = event.get("Records", [])
    if not records:
        log.warning("No records in event")
        return {"ok": True, "processed": 0}

    s3 = _s3_client()
    processed = 0
    errors = 0

    for rec in records:
        try:
            bucket = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]
            if not key.endswith(".gz"):
                log.info(f"Skipping non-gz: {key}")
                continue

            provider, branch, type_, iso_ts = parse_key(key)

            obj = s3.get_object(Bucket=bucket, Key=key)
            gz_bytes = obj["Body"].read()
            xml_bytes = decompress_gz_to_bytes(gz_bytes)

            items = extract_items_from_bytes(xml_bytes, type_)
            payload = {
                "provider": provider,
                "branch": branch,
                "type": type_,
                "timestamp": iso_ts,
                "items": items,
            }

            # save locally in Lambda's /tmp (or OS temp on Windows)
            safe_key = key.replace("/", "__")
            TMP_DIR = (os.getenv("LAMBDA_TMP")
                       or os.getenv("TMPDIR")
                       or ("/tmp" if os.name != "nt" else tempfile.gettempdir()))
            os.makedirs(TMP_DIR, exist_ok=True)
            out_path = os.path.join(TMP_DIR, f"{safe_key}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            log.info(f"Saved local JSON to {out_path}")

            # queue (optional)
            if _queue_enabled():
                try:
                    send_message(payload)
                except Exception as qe:
                    log.warning(f"Queue send failed (skipping): {qe}")

            # last-run marker
            try:
                upsert_last_run(provider, branch, type_, iso_ts)
            except Exception as de:
                log.warning(f"DB upsert failed (continuing): {de}")

            processed += 1

        except Exception as e:
            errors += 1
            log.error(f"Error: {e}\n{traceback.format_exc()}")

    return {"ok": errors == 0, "processed": processed, "errors": errors}

# ---------- Local simulator (used by local_run.py) ----------
def process_local_file(file_path: str):
    """
    Process a local .gz file under:
      <S3_SIMULATOR_ROOT>/providers/<provider>/<branch>/<filename>.gz
    Builds payload JSON, saves to extractor_lambda/outbox, optionally sends to queue,
    and marks last-run state.
    """
    p = Path(file_path).resolve()
    root = Path(S3_SIMULATOR_ROOT).resolve() / "providers"
    rel = p.relative_to(root)
    key = f"providers/{str(rel).replace(os.sep, '/')}"  # S3-like key

    provider, branch, type_, iso_ts = parse_key(key)

    with open(p, "rb") as f:
        gz_bytes = f.read()

    xml_bytes = decompress_gz_to_bytes(gz_bytes)
    items = extract_items_from_bytes(xml_bytes, type_)

    payload = {
        "provider": provider,
        "branch": branch,
        "type": type_,
        "timestamp": iso_ts,
        "items": items,
    }

    # Save to outbox
    outbox = Path(__file__).parent / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)
    safe_ts = iso_ts.replace(":", "").replace("-", "").replace("T", "_").replace("Z", "")
    out_name = f"{provider}_{branch}_{type_}_{safe_ts}.json"
    out_path = outbox / out_name
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(f"[local] saved: {out_path}")

    # Queue (optional)
    if _queue_enabled():
        try:
            send_message(payload)
        except Exception as qe:
            log.warning(f"[local] queue send failed (skipping): {qe}")

    # State/DB
    try:
        upsert_last_run(provider, branch, type_, iso_ts)
    except Exception as de:
        log.warning(f"[local] DB upsert failed (continuing): {de}")

    return payload
