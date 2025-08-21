import os, re, json, gzip, io, datetime, time, boto3
import xml.etree.ElementTree as ET
from botocore.exceptions import ClientError

# ===== קונפיג בסיסי (LocalStack) =====
S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://localstack:4566')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
S3_BUCKET = os.getenv('S3_BUCKET', 'test-bucket')


from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    config=Config(s3={"addressing_style": "path"})  # חשוב ל-LocalStack
)

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
        return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
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
        "provider": provider,   # מהמופיע gz: ChainId
        "branch": branch,       # מהמופיע gz: StoreId
        "type": data_type,      # pricesFull / promoFull
        "timestamp": timestamp, # לפי השעון שבשם הקובץ
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
            print(f"Processed {key} -> s3://{bucket}/{json_key} ({len(doc.get('items', []))} records)")
            outputs.append({"key": key, "ok": True, "json_key": json_key, "count": len(doc.get("items", []))})
        except Exception as e:
            print(f"Error processing {key}: {e}")
            outputs.append({"key": key, "ok": False, "reason": str(e)})
    return {'statusCode': 200, 'body': json.dumps(outputs, ensure_ascii=False)}



if __name__ == "__main__":
    print("Lambda poller started. Scanning S3 every 10s …")
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
