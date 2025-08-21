import os, re, json, gzip, io, datetime, time, boto3
import xml.etree.ElementTree as ET
from botocore.exceptions import ClientError
from botocore.config import Config


# i saw people online says its better to wrap this import with "try" so the script wont harbu darbu if the import fails, and the code will still run
try:
    import pika
except Exception:
    pika = None

try:
    from pymongo import MongoClient, ASCENDING
    from pymongo.errors import PyMongoError
except Exception:
    MongoClient = None
    ASCENDING = None
    PyMongoError = Exception

S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://localstack:4566')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
S3_BUCKET = os.getenv('S3_BUCKET', 'test-bucket')

QUEUE_BACKEND = os.getenv('QUEUE_BACKEND', 'rabbit').lower()  # <-- ברירת מחדל: rabbit
RABBIT_URL = os.getenv('RABBIT_URL', 'amqp://guest:guest@rabbitmq:5672/%2f')
RABBIT_QUEUE = os.getenv('RABBIT_QUEUE', 'results-queue')

STATE_BACKEND = os.getenv('STATE_BACKEND', 'mongo').lower()
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017')
MONGO_DB = os.getenv('MONGO_DB', 'prices')
MONGO_COL = os.getenv('MONGO_COL', 'last_run')

aws_cfg = dict(
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    config=Config(s3={"addressing_style": "path"})
)
s3 = boto3.client("s3", **aws_cfg)

_rabbit_conn = None
_rabbit_channel = None

_mongo_col = None

def ensure_bucket(bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"[Init] Created S3 bucket: {bucket}")

def ensure_rabbit():
    global _rabbit_conn, _rabbit_channel
    if QUEUE_BACKEND != "rabbit":
        return None
    if pika is None:
        raise RuntimeError("pika not installed — cannot use RabbitMQ")
    _rabbit_conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    _rabbit_channel = _rabbit_conn.channel()
    _rabbit_channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
    try:
        _rabbit_channel.confirm_delivery()
    except Exception:
        pass
    print(f"[Init] RabbitMQ queue declared: {RABBIT_QUEUE}")
    return _rabbit_channel

def ensure_mongo():
    global _mongo_col
    if STATE_BACKEND != "mongo":
        return None
    if MongoClient is None:
        raise RuntimeError("pymongo not installed — cannot use MongoDB")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COL]
    col.create_index(
        [("provider", ASCENDING), ("branch", ASCENDING), ("type", ASCENDING)],
        unique=True,
    )
    _mongo_col = col
    print(f"[Init] Mongo ready: {MONGO_URI} db={MONGO_DB} col={MONGO_COL}")
    return _mongo_col

# gz validation (because before that i got the html file instead of gz file)
def _download_gz(bucket: str, key: str) -> io.BytesIO:
    obj = s3.get_object(Bucket=bucket, Key=key)
    gz_bytes = obj["Body"].read()
    first16 = gz_bytes[:16]
    print(f"[S3] {key} first bytes: {first16!r}")
    if len(gz_bytes) < 2 or gz_bytes[:2] != b"\x1f\x8b":
        raise ValueError(f"Not a gz header: {first16!r}")
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes)) as gz:
        xml_bytes = gz.read()
    return io.BytesIO(xml_bytes)

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

def _save_json(bucket: str, src_key: str, doc: dict) -> str:
    json_key = re.sub(r'\.gz$', '.json', src_key)
    body = json.dumps(doc, ensure_ascii=False).encode('utf-8')
    s3.put_object(Bucket=bucket, Key=json_key, Body=body, ContentType='application/json')
    return json_key

# --- Parsers ---
def parse_pricefull(xml_stream):
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

def parse_promofull(xml_stream):
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

# --- Emit full JSON to RabbitMQ ---
def _emit_event_full_json(doc: dict):
    if QUEUE_BACKEND != "rabbit":
        print(f"[Queue] Skipped (QUEUE_BACKEND={QUEUE_BACKEND})")
        return
    if not _rabbit_channel:
        raise RuntimeError("Rabbit channel not initialized")
    payload = json.dumps(doc, ensure_ascii=False).encode("utf-8")
    _rabbit_channel.basic_publish(
        exchange="",
        routing_key=RABBIT_QUEUE,
        body=payload,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        ),
        mandatory=False,
    )

def _update_last_run(provider: str, branch: str, data_type: str, last_ts_iso: str):
    now_iso = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    if STATE_BACKEND == "mongo":
        if _mongo_col is None:
            raise RuntimeError("Mongo collection not initialized")
        _mongo_col.update_one(
            {"provider": provider, "branch": branch, "type": data_type},
            {"$set": {"last_run": last_ts_iso, "updated_at": now_iso}},
            upsert=True,
        )
    else:
        print(f"[State] Skipped (STATE_BACKEND={STATE_BACKEND})")

def process_s3_object_to_json(bucket: str, key: str):
    m = KEY_RE.match(key)
    if not m:
        print(f"Skipping {key} – path not matching")
        return None
    data_type = m.group('type')
    ts = m.group('ts')
    timestamp = _iso_from_ts(ts)
    xml_stream = _download_gz(bucket, key)

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

def lambda_handler(event, context=None):
    if 'Records' not in event:
        return {'statusCode': 400, 'body': json.dumps("No Records key")}
    outputs = []
    for rec in event['Records']:
        bucket = rec['s3']['bucket']['name']
        key = rec['s3']['object']['key']
        try:
            doc = process_s3_object_to_json(bucket, key)
            if not doc:
                outputs.append({"key": key, "ok": False, "reason": "invalid format"})
                continue
            json_key = _save_json(bucket, key, doc)

            # שליחת ה-JSON המלא לתור RabbitMQ
            try:
                _emit_event_full_json(doc)
            except Exception as qe:
                print(f"[Queue] Failed to emit event for {key}: {qe}")

            # עדכון זמן ריצה אחרון במונגו
            try:
                _update_last_run(
                    doc.get("provider") or "",
                    doc.get("branch") or "",
                    doc.get("type") or "",
                    doc.get("timestamp") or "",
                )
            except Exception as se:
                print(f"[State] Failed to update last_run for {key}: {se}")

            print(f"Processed {key} -> s3://{bucket}/{json_key} ({len(doc.get('items', []))} records)")
            outputs.append({"key": key, "ok": True, "json_key": json_key, "count": len(doc.get("items", []))})
        except Exception as e:
            print(f"Error processing {key}: {e}")
            outputs.append({"key": key, "ok": False, "reason": str(e)})
    return {'statusCode': 200, 'body': json.dumps(outputs, ensure_ascii=False)}

# main: poller mode
if __name__ == "__main__":
    print("Lambda poller started. Scanning S3 every 10s …")

    try:
        ensure_bucket(S3_BUCKET)
    except Exception as e:
        print(f"[Init] Bucket ensure failed: {e}")

    try:
        ensure_rabbit()
    except Exception as e:
        print(f"[Init] Queue ensure failed: {e}")

    try:
        if STATE_BACKEND == "mongo":
            ensure_mongo()
    except Exception as e:
        print(f"[Init] State ensure failed: {e}")

    seen_keys = set()
    while True:
        try:
            resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="providers/")
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".gz") and key not in seen_keys:
                    print(f"New file detected: {key}")
                    event = {
                        "Records": [{
                            "eventName": "ObjectCreated:Put",
                            "s3": {"bucket": {"name": S3_BUCKET}, "object": {"key": key}}
                        }]
                    }
                    lambda_handler(event)
                    seen_keys.add(key)
        except Exception as e:
            print(f"Poll error: {e}")
        time.sleep(10)
