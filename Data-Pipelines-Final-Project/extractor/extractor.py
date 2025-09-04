import os, json, ntpath, logging, boto3, time, xml.etree.ElementTree as ET, re
from datetime import datetime, timezone
from urllib.parse import unquote_plus
from io_utils import provider_from_key, branch_from_key, read_and_decompress_gz

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", os.getenv("AWS_REGION", "eu-central-1"))
AWS_EP     = os.getenv("AWS_ENDPOINT_URL") or os.getenv("S3_ENDPOINT") or "http://localstack:4566"

session = boto3.session.Session(region_name=AWS_REGION)
s3  = session.client("s3",  endpoint_url=AWS_EP)
sqs = session.client("sqs", endpoint_url=AWS_EP)
ddb = session.resource("dynamodb", endpoint_url=AWS_EP)

BUCKET      = os.getenv("S3_BUCKET", "price-data")
OUT_BUCKET  = os.getenv("OUT_BUCKET", "govil-price-lists")
OUT_PREFIX  = os.getenv("OUT_PREFIX", "processed-json")

_account = os.getenv("AWS_ACCOUNT_ID", "000000000000")
_base    = (os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566").rstrip("/"))

SQS_IN_URL = (
    os.getenv("SQS_IN_URL")
    or os.getenv("SQS_IN_QUEUE_URL")
    or f"{_base}/{_account}/price-extractor-in"
)
SQS_OUT_URL = (
    os.getenv("SQS_OUT_URL")
    or os.getenv("SQS_QUEUE_URL")
    or f"{_base}/{_account}/price-extractor-events"
)

DDB_TABLE = os.getenv("DDB_TABLE", "price_extractor_runs")
_table = ddb.Table(DDB_TABLE)

logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger(__name__)

def _clean_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def _extract_barcode(el: ET.Element) -> str:

    CANDIDATE_TAGS = [
        "Barcode", "ItemBarCode",
        "ItemCode", "ItemID", "ItemId", "Code",
        "ProductId", "ProductID",
    ]
    for t in CANDIDATE_TAGS:
        v = el.findtext(f".//{t}")
        if not v:
            continue
        digits = _clean_digits(v)
        if 7 <= len(digits) <= 20:
            return digits
    return ""

def _to_float(v: str | None) -> float:
    if v is None:
        return 0.0
    s = str(v).replace(",", ".").replace("₪", "").strip()
    try:
        return float(s)
    except Exception:
        return 0.0

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

TS_PAT = re.compile(r"-([0-9]{12})\.g?z$", re.IGNORECASE)

def ts_from_key(key: str) -> datetime:
    m = TS_PAT.search(key or "")
    if not m:
        return datetime.now(timezone.utc)
    s = m.group(1)
    try:
        return datetime.strptime(s, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

def checkpoint_pk(provider: str, branch: str, data_type: str) -> str:
    return f"{provider}#{branch}#{data_type}"

def get_last_checkpoint(provider: str, branch: str, data_type: str) -> datetime | None:
    try:
        resp = _table.get_item(Key={"pk": checkpoint_pk(provider, branch, data_type)})
        item = resp.get("Item")
        if item and item.get("last_ts_iso"):
            return datetime.fromisoformat(item["last_ts_iso"])
    except Exception:
        log.debug("no checkpoint yet for %s/%s/%s", provider, branch, data_type)
    return None

def set_checkpoint(provider: str, branch: str, data_type: str, last_ts: datetime, src_key: str):
    try:
        _table.put_item(
            Item={
                "pk": checkpoint_pk(provider, branch, data_type),
                "last_ts_iso": last_ts.isoformat(),
                "last_src_key": src_key,
                "updated_at": _now_iso(),
            }
        )
    except Exception as e:
        log.warning("failed updating checkpoint: %s", e)

def write_output_json(src_key: str, provider: str, branch: str, doc: dict) -> str:

    src_base = ntpath.basename(src_key).rsplit(".", 1)[0]
    out_key  = f"{OUT_PREFIX}/{provider}/{branch}/{doc['type']}/{src_base}.json"
    s3.put_object(
        Bucket=OUT_BUCKET,
        Key=out_key,
        Body=json.dumps(doc, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("wrote JSON → s3://%s/%s (%d items)", OUT_BUCKET, out_key, len(doc.get("items", [])))
    return out_key

def parse_price_items(root: ET.Element, provider: str):
    items = []
    name_tags  = ("ItemName", "ItemNm", "Name")
    price_tags = ("ItemPrice", "Price")
    unit_tags  = ("UnitOfMeasure", "Unit")

    for el in root.findall(".//Item"):
        name = None
        for t in name_tags:
            v = el.findtext(t)
            if v:
                name = v.strip()
                break
        if not name:
            name = "unknown"

        price = 0.0
        for t in price_tags:
            v = el.findtext(t)
            if v:
                price = _to_float(v)
                break

        unit = ""
        for t in unit_tags:
            v = el.findtext(t)
            if v:
                unit = v.strip()
                break

        row = {"product": name, "price": price, "unit": unit}
        bc = _extract_barcode(el)
        if bc:
            row["barcode"] = bc

        items.append(row)

    return items

N_FOR_PRICE_RE = re.compile(
    r"(?P<n>\d+)\s*(?:יח'?|יחידות|x|X)?\s*ב[-\s]*₪?\s*(?P<total>\d+(?:[.,]\d+)?)"
)

def _derive_promo_unit_price(desc: str, discounted: float) -> float:

    m = N_FOR_PRICE_RE.search(desc or "")
    if not m:
        return discounted
    try:
        n = int(m.group("n"))
        total = _to_float(m.group("total"))
        if n > 0 and total > 0:
            return round(total / n, 2)
    except Exception:
        pass
    return discounted

def _collect_promo_barcodes(promo_el: ET.Element) -> list[str]:

    bcs: set[str] = set()
    for it in promo_el.findall(".//Item"):
        bc = _extract_barcode(it)
        if bc:
            bcs.add(bc)
    return list(bcs)

def parse_promo_items(root: ET.Element, provider: str):

    promos: list[dict] = []
    for promo in root.findall(".//Promotion"):
        desc = (
            promo.findtext("PromotionDescription")
            or promo.findtext("Description")
            or promo.findtext("Name")
            or "unknown"
        ).strip()

        base = _to_float(promo.findtext("DiscountedPrice") or promo.findtext("Price") or "0")
        unit_price = _derive_promo_unit_price(desc, base)
        bcs = _collect_promo_barcodes(promo)

        if bcs:
            for bc in bcs:
                promos.append({
                    "product": desc,
                    "price": unit_price,
                    "unit": "unit",
                    "barcode": bc,
                    "promo_text": desc,
                })
        else:
            promos.append({
                "product": desc,
                "price": unit_price,
                "unit": "unit",
                "promo_text": desc,
            })

    return promos

def type_from_key(key: str) -> str:
    k = (key or "").lower()
    return "promoFull" if "promo" in k else "pricesFull"

def handler(event, context):
    log.info("price-extractor invoked")

    for rec in event.get("Records", []):
        s3evt = rec.get("s3", {})
        if not s3evt:
            continue

        bucket = s3evt.get("bucket", {}).get("name", BUCKET)
        key    = unquote_plus(s3evt.get("object", {}).get("key", ""))
        etag   = s3evt.get("object", {}).get("eTag")
        provider  = provider_from_key(key)
        branch    = branch_from_key(key)
        data_type = type_from_key(key)
        key_ts    = ts_from_key(key)

        last = get_last_checkpoint(provider, branch, data_type)
        if last and key_ts <= last:
            log.info("⏭️  skip older/equal file (checkpoint=%s) %s", last.isoformat(), key)
            continue

        try:
            xml_bytes = read_and_decompress_gz(bucket, key)
            root      = ET.fromstring(xml_bytes)
        except Exception as e:
            log.exception("Failed reading/parsing %s from s3://%s: %s", key, bucket, e)
            continue

        if data_type == "pricesFull":
            items = parse_price_items(root, provider)
        else:
            items = parse_promo_items(root, provider)

        doc = {
            "provider":  provider,
            "branch":    branch,
            "type":      data_type,
            "timestamp": _now_iso(),
            "src_key":   key,
            "etag":      etag,
            "items":     items,
        }

        log.info("✅ built doc (%s): %d items for %s", data_type, len(items), key)

        out_key = None
        try:
            out_key = write_output_json(key, provider, branch, doc)
        except Exception as e:
            log.exception("Failed writing processed JSON for %s: %s", key, e)

        if out_key:
            try:
                pointer = {
                    "provider": provider,
                    "branch": branch,
                    "type": data_type,
                    "timestamp": _now_iso(),
                    "s3": {"bucket": OUT_BUCKET, "key": out_key},
                    "count": len(items),
                }
                sqs.send_message(QueueUrl=SQS_OUT_URL, MessageBody=json.dumps(pointer))
                log.info("→ sent pointer to SQS OUT (%s)", SQS_OUT_URL)
            except Exception as e:
                log.exception("Failed sending to OUT SQS for %s: %s", key, e)

        set_checkpoint(provider, branch, data_type, key_ts, key)

    return {"ok": True}

def poll():
    log.info("Extractor poller started; IN=%s  OUT=%s", SQS_IN_URL, SQS_OUT_URL)
    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=SQS_IN_URL,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20,
                VisibilityTimeout=60,
            )
            for m in resp.get("Messages", []):
                try:
                    body  = m["Body"]
                    event = json.loads(body) if body and body[0] in "{[" else {"body": body}
                    handler(event, None)
                    sqs.delete_message(QueueUrl=SQS_IN_URL, ReceiptHandle=m["ReceiptHandle"])
                except Exception:
                    log.exception("Failed to process message %s", m.get("MessageId"))
            if not resp.get("Messages"):
                time.sleep(2)
        except Exception:
            log.exception("Receive loop error")
            time.sleep(5)

if __name__ == "__main__":
    log.info("Starting extractor…")
    try:
        ddb_client = session.client("dynamodb", endpoint_url=AWS_EP)
        ddb_client.describe_table(TableName=DDB_TABLE)
    except Exception:
        try:
            ddb_client.create_table(
                TableName=DDB_TABLE,
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
            ddb_client.get_waiter("table_exists").wait(TableName=DDB_TABLE)
            log.info("created DynamoDB table: %s", DDB_TABLE)
        except Exception as e:
            log.warning("could not create ddb table %s: %s", DDB_TABLE, e)
    poll()
