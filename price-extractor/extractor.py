# extractor.py
import os
import re
import json
import ntpath
import logging
import boto3
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import unquote_plus

from io_utils import read_and_decompress_gz

# --- AWS clients ---
s3 = boto3.client("s3")
sqs = boto3.client("sqs")
dynamo = boto3.client("dynamodb")

# --- Env vars ---
BUCKET = os.environ.get("BUCKET_NAME")
QUEUE_URL = os.environ["SQS_QUEUE_URL"]
DDB_TABLE = os.environ.get("DDB_TABLE")

OUT_BUCKET = os.environ.get("OUT_BUCKET", "govil-price-lists")
OUT_PREFIX = os.environ.get("OUT_PREFIX", "processed-json")

logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger(__name__)


# ---------- JSON writer (NEW) ----------
def write_output_json(src_key: str, provider: str, branch: str, doc: dict):

    src_base = ntpath.basename(src_key).rsplit(".", 1)[0]
    out_key = f"{OUT_PREFIX}/{provider}/{branch}/{doc['type']}/{src_base}.json"

    s3.put_object(
        Bucket=OUT_BUCKET,
        Key=out_key,
        Body=json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("wrote JSON → s3://%s/%s (%d items)", OUT_BUCKET, out_key, len(doc.get("items", [])))


# ---------- Lambda handler ----------
def handler(event, context):
    log.info("price-extractor invoked")
    log.info("RAW EVENT: %s", json.dumps(event, ensure_ascii=False))

    for rec in event.get("Records", []):
        s3evt = rec.get("s3", {})
        if not s3evt:
            continue

        bucket = s3evt.get("bucket", {}).get("name", BUCKET)
        key = unquote_plus(s3evt.get("object", {}).get("key", ""))
        etag = s3evt.get("object", {}).get("eTag")

        try:
            xml_bytes = read_and_decompress_gz(bucket, key)
            root = ET.fromstring(xml_bytes)
        except Exception as e:
            log.exception("Failed reading/parsing %s: %s", key, e)
            continue

        provider = provider_from_key(key)
        branch = branch_from_key(key)
        data_type = type_from_key(key)

        # Parse items
        if data_type == "pricesFull":
            items = parse_price_items(root, provider)
        else:
            items = parse_promo_items(root, provider)

        doc = {
            "provider": provider,
            "branch": branch,
            "type": data_type,
            "timestamp": utc_now(),
            "items": items,
        }

        log.info("✅ built doc (%s): %d items", data_type, len(items))

        #Write normalized JSON artifact to S3
        try:
            write_output_json(key, provider, branch, doc)
        except Exception as e:
            log.exception("Failed writing processed JSON for %s: %s", key, e)

        # 2) Send to SQS
        try:
            sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(doc))
        except Exception as e:
            log.exception("Failed sending to SQS for %s: %s", key, e)
            continue

        # 3) Update last-run marker in DynamoDB
        try:
            if DDB_TABLE and etag:
                dynamo.put_item(
                    TableName=DDB_TABLE,
                    Item={
                        "pk": {"S": f"{provider}#{branch}#{data_type}"},
                        "last_object_key": {"S": key},
                        "last_etag": {"S": etag},
                        "last_timestamp": {"S": doc["timestamp"]},
                    },
                )
        except Exception as e:
            log.exception("Failed updating DynamoDB marker for %s: %s", key, e)

    return {"ok": True}


# ---------- helpers ----------
def utc_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def provider_from_key(key: str) -> str:
    return key.split("/", 1)[0] if "/" in key else "unknown"


def branch_from_key(key: str) -> str:
    # …-<branch>-<yyyymmdd…>.gz  → capture the middle 3 digits
    m = re.search(r"-([0-9]{3})-", key)
    return m.group(1) if m else "000"


def type_from_key(key: str) -> str:

    k = key.lower()
    if "promo" in k:
        return "promoFull"
    return "pricesFull"


# ---------- parsing----------
def parse_price_items(root: ET.Element, provider: str):
    items = []
    if provider == "yohananof":
        for item in root.findall(".//Item"):
            name = (item.findtext("ItemName") or "unknown").strip()
            price_text = item.findtext("ItemPrice") or "0"
            unit = (item.findtext("UnitOfMeasure") or "").strip()
            try:
                price = float(price_text)
            except Exception:
                price = 0.0
            items.append({"product": name, "price": price, "unit": unit})
    else:
        # kingstore, maayan
        for item in root.findall(".//Item"):
            name = (item.findtext("ItemNm") or "unknown").strip()
            price_text = item.findtext("ItemPrice") or "0"
            unit = (item.findtext("UnitOfMeasure") or "").strip()
            try:
                price = float(price_text)
            except Exception:
                price = 0.0
            items.append({"product": name, "price": price, "unit": unit})
    return items


def parse_promo_items(root: ET.Element, provider: str):
    promos = []
    if provider == "yohananof":
        for promo in root.findall(".//Promotion"):
            desc = (promo.findtext("PromotionDescription") or "unknown").strip()
            price_text = promo.findtext("DiscountedPrice") or "0"
            try:
                price = float(price_text)
            except Exception:
                price = 0.0
            promos.append({"product": desc, "price": price, "unit": "unit"})
    else:
        for promo in root.findall(".//Promotion"):
            desc = (
                promo.findtext("PromotionDescription")
                or promo.findtext("Description")
                or "unknown"
            ).strip()
            price_text = (promo.findtext("DiscountedPrice") or promo.findtext("Price") or "0")
            try:
                price = float(price_text)
            except Exception:
                price = 0.0
            promos.append({"product": desc, "price": price, "unit": "unit"})
    return promos
