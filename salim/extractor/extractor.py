import os
import re
import gzip
import json
import boto3
import argparse
import xml.etree.ElementTree as ET

from botocore.config import Config
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
OUT_ROOT = Path(os.getenv("EXTRACTOR_OUT_DIR", BASE_DIR / "extractor_data"))
MANIFEST_PATH = Path(os.getenv("EXTRACTOR_MANIFEST", BASE_DIR / "manifest" / "manifest.json"))

EXTRACTOR_DEBUG = os.getenv("EXTRACTOR_DEBUG") == "1"

BASE_ENDPOINT = os.getenv("AWS_ENDPOINT_URL")
if BASE_ENDPOINT and not BASE_ENDPOINT.startswith("http"):
    BASE_ENDPOINT = f"http://{BASE_ENDPOINT}:4566"

DEFAULT_ON_DOCKER = "http://localstack:4566"
DEFAULT_ON_HOST   = "http://localhost:4566"

FALLBACK = BASE_ENDPOINT or DEFAULT_ON_DOCKER

S3_ENDPOINT = os.getenv("S3_ENDPOINT",  FALLBACK or DEFAULT_ON_HOST)
SQS_ENDPOINT = os.getenv("SQS_ENDPOINT", FALLBACK or DEFAULT_ON_HOST)
DDB_ENDPOINT = os.getenv("DDB_ENDPOINT", FALLBACK or DEFAULT_ON_HOST)
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

S3_BUCKET = os.getenv("S3_BUCKET", "test-bucket")
OUT_QUEUE = os.getenv("OUT_QUEUE", "price-events")
MAX_ITEMS_PER_MESSAGE = int(os.getenv("MAX_ITEMS_PER_MESSAGE", "250"))
DDB_TABLE = os.getenv("DDB_TABLE", "last_run")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    config=Config(s3={"addressing_style": "path"}),
)

sqs = boto3.client(
    "sqs",
    endpoint_url=SQS_ENDPOINT,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
)

ddb = boto3.resource(
    "dynamodb",
    endpoint_url=DDB_ENDPOINT,
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
)
DDB_TABLE_NAME = os.getenv("DDB_TABLE", "last_run")
ddb_table = ddb.Table(DDB_TABLE_NAME)

def ddb_get_last(id_: str):
    try:
        resp = ddb_table.get_item(Key={"id": id_})
        item = resp.get("Item")
        if item:
            return item.get("last_ts"), item
    except ClientError as e:
        print(f"[WARN] DDB get_item failed for {id_}: {e}")
    return None, None

def ddb_update_last(id_: str, ts: str, key: str, json_name: str):
    try:
        ddb_table.put_item(
            Item={
                "id": id_,
                "last_ts": ts,
                "last_key": key,
                "json": json_name,
                "updated_at": datetime.now(timezone.utc).isoformat()
            },
            ConditionExpression="attribute_not_exists(#id) OR #ts < :ts",
            ExpressionAttributeNames={"#id": "id", "#ts": "last_ts"},
            ExpressionAttributeValues={":ts": ts},
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            if EXTRACTOR_DEBUG:
                print(f"[DDB] Skip update for {id_}, stored ts is newer or equal")
            return False
        print(f"[ERR ] DDB put_item failed for {id_}: {e}")
        return False

_queue_url_cache = None
def get_queue_url():
    global _queue_url_cache
    if _queue_url_cache:
        return _queue_url_cache
    try:
        _queue_url_cache = sqs.get_queue_url(QueueName=OUT_QUEUE)["QueueUrl"]
    except ClientError:
        sqs.create_queue(QueueName=OUT_QUEUE)
        _queue_url_cache = sqs.get_queue_url(QueueName=OUT_QUEUE)["QueueUrl"]
    if EXTRACTOR_DEBUG:
        print(f"[SQS] Using queue: {OUT_QUEUE} -> {_queue_url_cache}")
    return _queue_url_cache


def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def send_doc_to_sqs(doc: dict):
    url = get_queue_url()
    base = {k: v for k, v in doc.items() if k not in ("items", "promotions")}
    sent_any = False

    def _send(payload, idx, parts):
        nonlocal sent_any
        body = json.dumps(payload, ensure_ascii=False)
        if EXTRACTOR_DEBUG:
            print(f"[SQS] about to send part {idx}/{parts}, size={len(body.encode('utf-8'))} bytes")
        try:
            resp = sqs.send_message(QueueUrl=url, MessageBody=body)
            sent_any = True
            if EXTRACTOR_DEBUG:
                print(f"[SQS] Sent part {idx}/{parts} msgId={resp.get('MessageId')} to {url}")
        except ClientError as e:
            print(f"[ERR ] SQS send failed: {e}")

    if "items" in doc:
        parts = list(_chunks(doc["items"], MAX_ITEMS_PER_MESSAGE))
        for idx, batch in enumerate(parts, 1):
            _send({**base, "items": batch, "part": idx, "parts": len(parts)}, idx, len(parts))

    elif "promotions" in doc:
        parts = list(_chunks(doc["promotions"], MAX_ITEMS_PER_MESSAGE))
        for idx, batch in enumerate(parts, 1):
            _send({**base, "promotions": batch, "part": idx, "parts": len(parts)}, idx, len(parts))

    if not sent_any and EXTRACTOR_DEBUG:
        print("[SQS] Nothing was sent (empty items/promotions)")


def get_keys_list(bucket, suffix =".gz"):
    keys = []
    token = None

    while True:
        kwargs = {"Bucket": bucket}

        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []) or []:
            k = obj["Key"]

            if k.endswith(suffix):
                keys.append(k)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")

        else:
            break

    return keys

def extract_xml(bucket, key):
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    body_object = s3_object["Body"].read()

    try:
        raw = gzip.decompress(body_object)

    except OSError:
        raw = body_object

    for enc in ("utf-8", "cp1255", "iso-8859-8"):
        try:
            return raw.decode(enc)
        except Exception:
            continue

    return raw.decode("utf-8", errors="replace")

def get_id_key(provider, branch, file_type):
    return f"{provider}_{branch}_{file_type}"

def get_timestamp(date, time):
    return f"{date}_{time}"

def get_text(node, tag):
    v = node.findtext(tag)
    return v.strip() if v else None

def find_tag(file, tag):
    root = ET.fromstring(file)
    tags = root.findall(f".//{tag}")
    return tags

def convert_items(items):
    out = []

    for item in items:
        code = get_text(item, "ItemCode")
        product = get_text(item, "ItemName") or get_text(item, "ManufacturerItemDescription") or ""
        price = get_text(item, "ItemPrice")
        unit = get_text(item, "UnitOfMeasure") or None

        if not (code and product and price):
            continue

        out.append({
            "code": code,
            "product": product,
            "price": price,
            "unit": unit
        })

    return {"items": out}

def convert_promotions(promotions):
    promotions_list = []

    for promotion in promotions:
        description = get_text(promotion, "PromotionDescription")
        start_date = get_text(promotion, "PromotionStartDate")
        start_time = get_text(promotion, "PromotionStartHour")
        end_date = get_text(promotion, "PromotionEndDate")
        end_time = get_text(promotion, "PromotionEndHour")
        discount_price = get_text(promotion, "DiscountedPrice")
        discount_rate = get_text(promotion, "DiscountRate")
        minimal_quantity = get_text(promotion, "MinQty")

        promotion_items = []
        for item in promotion.findall(".//Item"):
            code = get_text(item, "ItemCode")
            if code:
                promotion_items.append(code)

        promotions_list.append({
            "description": description or "",
            "start": f"{start_date}_{start_time}" if start_date and start_time else None,
            "end":   f"{end_date}_{end_time}" if end_date and end_time else None,
            "price": discount_price,
            "rate":  discount_rate,
            "quantity": minimal_quantity,
            "items": promotion_items
        })

    return {"promotions": promotions_list}

def convert_file(file, file_name, file_type):

    if file_type == "PriceFull":
        items = find_tag(file, "Item")
        return convert_items(items)

    elif file_type == "PromoFull":
        promotions = find_tag(file, "Promotion")
        return convert_promotions(promotions)

    else:
        print(f"File \"{file_name}\" is not match \"PriceFull\" or \"PromoFull\" format")
        return None

def split_s3_key(key):
    parts = key.split("/")
    provider, branch, file_name = parts
    return provider, branch, file_name

def parse_key(key):
    parts = key.split("/")
    if len(parts) < 3:
        raise ValueError(f"Unexpected key structure")

    provider = parts[0]
    branch   = parts[1]
    filename = parts[-1]

    matched_string = re.match(r"^(PriceFull|PromoFull)_([^_]+)_([^\.]+)\.gz$", filename, flags=re.IGNORECASE)
    if not matched_string:
        raise ValueError(f"Unexpected filename format: {filename}")

    file_type = "PriceFull" if matched_string.group(1).lower().startswith("price") else "PromoFull"
    date = matched_string.group(2)
    time = matched_string.group(3).replace("-", ":")

    return provider, branch, file_type, date, time

def main():

    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default=S3_BUCKET, help="S3 bucket name (default: env S3_BUCKET or test-bucket)")
    ap.add_argument("--only-key", default=None)
    args = ap.parse_args()

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(BASE_DIR / "stores" / "stores_mapping.json", "r",encoding="utf-8") as stores_file:
        stores_mapping = json.load(stores_file)

    if args.only_key:
        keys = [args.only_key]
    else:
        keys = get_keys_list(args.bucket, suffix=".gz")

    if not keys:
        print(f"S3 bucket is empty")
        return

    try:
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            processed_map = json.load(f).get("entries", {})
    except FileNotFoundError:
        processed_map = {}

    processed = 0
    all_files = 0
    for key in keys:

        try:
            provider, branch, file_type, date, time = parse_key(key)

        except Exception as exception:
            print(f"[WARN] Skip key (unrecognized): {key} ({exception})")
            all_files += 1
            continue

        id_key = get_id_key(provider, branch, file_type)
        current_timestamp = get_timestamp(date, time)

        last_ts, last_item = ddb_get_last(id_key)
        if last_ts is None and 'processed_map' in locals():
            prev = processed_map.get(id_key)
            if prev:
                last_ts = prev.get("timestamp")

        if last_ts and last_ts >= current_timestamp:
            if EXTRACTOR_DEBUG:
                print(
                    f"[SKIP] Not newer ({current_timestamp} <= {last_ts}) for {key}")
            all_files += 1
            continue

        branch_name = stores_mapping.get(provider, {}).get(branch, branch)

        try:
            xml_text = extract_xml(args.bucket, key)
            payload = convert_file(xml_text, key.rsplit("/", 1)[-1], file_type)
            if payload is None:
                continue

        except ClientError as exception:
            print(f"[ERR ] S3 error for {key}: {exception}")
            all_files += 1
            continue

        except Exception as exception:
            print(f"[ERR ] Convert error for {key}: {exception}")
            all_files += 1
            continue

        doc = {
            "provider": provider,
            "branch_code": branch,
            "branch": branch_name,
            "type": file_type,
            "timestamp": f"{date}_{time}",
        }
        doc.update(payload)

        n_items = len(payload.get("items", []))
        n_promos = len(payload.get("promotions", []))
        if EXTRACTOR_DEBUG:
            print(f"[DBG] Counts: items={n_items} promotions={n_promos} for {key}")

        if n_items == 0 and n_promos == 0:
            print(
                f"[WARN] No items/promotions parsed for {key}; skipping SQS send")
            processed += 1
            all_files += 1
            continue

        out_dir = OUT_ROOT
        out_dir.mkdir(parents=True, exist_ok=True)
        time_for_name = time.replace(":", "-")
        timestamp_for_name = f"{date}_{time_for_name}"
        out_path = out_dir / f"{provider}_{branch}_{file_type}_{timestamp_for_name}.json"

        processed_map[id_key] = {
            "key": key,
            "timestamp": current_timestamp,
            "json": out_path.name
        }

        if os.getenv("SAVE_LOCAL_JSON") == "1":
            out_dir = OUT_ROOT
            out_dir.mkdir(parents=True, exist_ok=True)
            time_for_name = time.replace(":", "-")
            ts_for_name = f"{date}_{time_for_name}"
            out_path = out_dir / f"{provider}_{branch}_{file_type}_{ts_for_name}.json"
            with open(out_path, "w", encoding="utf-8") as output_file:
                json.dump(doc, output_file, ensure_ascii=False, indent=2)

        send_doc_to_sqs(doc)

        _ = ddb_update_last(id_key, current_timestamp, key, out_path.name if 'out_path' in locals() else "")

        processed += 1
        all_files += 1

        print(f"[{processed}] Sent to SQS: {provider}_{branch}_{file_type}_{timestamp_for_name}.json")

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as manifest_file:
        json.dump({"bucket": args.bucket, "entries": processed_map}, manifest_file, ensure_ascii=False, indent=2)

    print(f"[Success] Extraction successfully finished\nProcessed files: {processed} out of {all_files}")

if __name__ == '__main__':
    main()