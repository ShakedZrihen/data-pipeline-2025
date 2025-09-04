import os
import json
import tempfile
import traceback
import logging, sys
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
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.getLogger().handlers = [handler]
logging.getLogger().setLevel(logging.INFO)
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

# ---------- XML helpers with Hebrew support ----------
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
                    if name_l.endswith(".gz"):
                        try:
                            data = gzip.decompress(data)
                        except Exception:
                            pass
                        name_l = name_l[:-3]  # "…xml.gz" -> "…xml"
                    if name_l.endswith(".xml"):
                        out.append(data)
        except Exception:
            out.append(raw)
    else:
        out.append(raw)
    return out
def _decode_smart(b: bytes) -> str:
    if b.startswith(b'\xef\xbb\xbf'):
        b = b[3:]
    for enc in ('utf-8', 'utf-16', 'windows-1255', 'cp1255', 'iso-8859-8'):
        try:
            s = b.decode(enc)
            if s.count('�') < 3:   # הימנע מטקסט פגום
                return s
        except UnicodeDecodeError:
            continue
    # fallback אחרון בלבד
    return b.decode('utf-8', errors='replace')


def _normalize_xml_encoding(data: bytes) -> bytes | str:
    """
    אם ה-XML ב-UTF-16 / עם NUL-Bytes – ננסה לפענח ל-utf-16 ולהחזיר str.
    אחרת נחזיר את הבייטים כפי שהם.
    """
    head = data[:6]
    if head.startswith(b"\xff\xfe") or head.startswith(b"\xfe\xff") or b"\x00" in head:
        try:
            return data.decode("utf-16", errors="replace")
        except Exception:
            return _decode_smart(data)
    # UTF-8 BOM
    if head.startswith(b"\xef\xbb\xbf"):
        return data[3:]
    return _decode_smart(data) if isinstance(data, bytes) else data

def _strip_ns(tag: str) -> str:
    # "{ns}ItemPrice" -> "ItemPrice"
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag

def _text(el):
    if el is None:
        return ""
    text = el.text or ""
    # ניקוי רווחים ותווי בקרה
    import re
    text = re.sub(r'\s+', ' ', text).strip()
    return text

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
def _xml_root_robust(xml_data):
    # 1) קודם כל נסה bytes ישירות – שומר על הקידוד שהוכרז ב-XML
    if isinstance(xml_data, (bytes, bytearray)):
        try:
            return ET.fromstring(xml_data)
        except ET.ParseError:
            pass

    # 2) אם נכשל, נסה פענוח חכם – אבל בלי "להחליף" לקידוד אחר בטקסט עצמו
    if isinstance(xml_data, (bytes, bytearray)):
        txt = _decode_smart(xml_data)  # תחזירי str
    else:
        txt = xml_data

    # אל תשני את ה-prolog ל-utf-8 כאן; פשוט תני ל-ET לקבל str
    return ET.fromstring(txt)


def parse_price_xml(xml_data):
    """Extract items from PriceFull-like XMLs into [{'product','price','unit'}, ...]."""
    items_out = []
    root = _xml_root_robust(xml_data)

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
            # ניקוי מחיר - כל הסימנים הנפוצים של שקל
            pprice_txt = pprice_txt.replace("₪", "").replace("ש\"ח", "").replace("שח", "")
            pprice_txt = pprice_txt.replace(",", "").replace(" ", "").strip()
            try:
                pprice = float(pprice_txt)
            except:
                pprice = None

        if pname or pprice is not None:
            items_out.append({"product": pname, "price": pprice, "unit": punit})
    return items_out

def parse_promo_xml(xml_data):
    """Extract items from PromoFull-like XMLs."""
    items_out = []
    root = _xml_root_robust(xml_data)

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
            # ניקוי מחיר
            price_txt = price_txt.replace("₪", "").replace("ש\"ח", "").replace("שח", "")
            price_txt = price_txt.replace(",", "").replace(" ", "").strip()
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

def _save_json_with_hebrew(data, filepath):
    """שמירת JSON עם תמיכה נכונה בעברית"""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
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
            
            # שימוש בפונקציה החדשה לשמירת JSON
            _save_json_with_hebrew(payload, out_path)
            log.info(f"Saved local JSON to {out_path}")

            # queue
            if _queue_enabled():
                try:
                    # שמירה לבדיקה ידנית
                    with open("items_sample.json", "w", encoding="utf-8") as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)

                    send_message(payload, outbox_path=str(out_path))

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
    key = f"providers/{str(rel).replace(os.sep, '/')}"

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
    
    # שימוש בפונקציה החדשה לשמירת JSON
    _save_json_with_hebrew(payload, out_path)
    log.info(f"[local] saved: {out_path}")

    # Queue
    if _queue_enabled():
        try:
            # שמירה לבדיקה ידנית
            with open("items_sample.json", "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            send_message(payload, outbox_path=str(out_path))

        except Exception as qe:
            log.warning(f"[local] queue send failed (skipping): {qe}")

    # State/DB
    try:
        upsert_last_run(provider, branch, type_, iso_ts)
    except Exception as de:
        log.warning(f"[local] DB upsert failed (continuing): {de}")

    return payload


log = logging.getLogger()
log.setLevel(logging.INFO)

def _print_preview(items, max_items=5):
    for it in items[:max_items]:
        log.info("  - %s | price=%s | unit=%s",
                 it.get("product"), it.get("price"), it.get("unit"))
    if len(items) > max_items:
        log.info("  ... and %d more", len(items) - max_items)

def handler(event, context=None):
    """
    SQS trigger: עובר על Records, קורא את ה-body (JSON),
    ואם זו 'מעטפה' גדולה עם outbox_path – קורא משם את הקובץ המלא ומדפיס דוגמית.
    """
    records = event.get("Records", [])
    for rec in records:
        body = rec.get("body", "{}")
        try:
            msg = json.loads(body)
        except Exception:
            log.warning("Non-JSON body: %s", body[:200])
            continue

        provider = msg.get("provider")
        branch   = msg.get("branch")
        type_    = msg.get("type")
        ts       = msg.get("timestamp")
        total    = msg.get("items_total")
        outbox   = msg.get("outbox_path")
        items    = msg.get("items_sample") or msg.get("items") or []

        log.info("SQS message: provider=%s branch=%s type=%s ts=%s total=%s",
                 provider, branch, type_, ts, total if total is not None else len(items))

        if not items and outbox and os.path.exists(outbox):
            try:
                with open(outbox, "r", encoding="utf-8") as f:
                    full = json.load(f)
                items = full.get("items", [])
                log.info("Loaded full JSON from outbox_path (%s), items=%d", outbox, len(items))
            except Exception as e:
                log.warning("Failed reading outbox_path: %s", e)

        _print_preview(items)

    return {"ok": True, "processed": len(records)}