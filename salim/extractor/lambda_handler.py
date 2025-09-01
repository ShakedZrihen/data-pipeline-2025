import os, io, gzip, json, logging
from typing import Tuple
import boto3

from utils.xml_utils import parse_xml_items, iso_from_filename
from sqs_producer import SQSProducer
from db_state import LastRunStore

log = logging.getLogger()
log.setLevel(logging.INFO)

s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
producer = SQSProducer(queue_url=os.environ.get("SQS_QUEUE_URL"))
state = LastRunStore(table_name=os.environ.get("DDB_TABLE"))

def parse_key(key: str) -> Tuple[str, str, str, str]:
    """
    Expect: providers/<provider>/<branch>/pricesFull_...gz or promoFull_...gz
    Returns (provider, branch, type, filename)
    """
    parts = key.split("/")
    idx = 1 if parts and parts[0].lower() == "providers" else 0
    provider = parts[idx] if len(parts) > idx else ""
    branch = parts[idx + 1] if len(parts) > idx + 1 else ""
    fname = parts[-1].lower() if parts else ""

    if fname.startswith("pricesfull"):
        typ = "pricesFull"
    elif fname.startswith("promofull"):
        typ = "promoFull"
    else:
        typ = "unknown"
    return provider, branch, typ, fname

def handler(event, context):
    """
    S3 event → read .gz → parse XML → build payload → send to SQS → update DynamoDB
    """
    records = event.get("Records", [])
    for rec in records:
        try:
            bkt = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]
            log.info("Processing s3://%s/%s", bkt, key)

            # 1) fetch + decompress
            obj = s3.get_object(Bucket=bkt, Key=key)
            with gzip.GzipFile(fileobj=obj["Body"]) as gz:
                xml = gz.read().decode("utf-8", errors="ignore")

            # 2) parse XML to items
            items = parse_xml_items(xml)
            if not items:
                log.warning("No items parsed: %s", key)
                continue

            # 3) derive meta from key + timestamp
            provider, branch, typ, fname = parse_key(key)
            ts_iso = iso_from_filename(fname)

            # 4) payload
            payload = {
                "provider": provider,
                "branch": branch,
                "type": typ,
                "timestamp": ts_iso,
                "source_key": key,
                "item_count": len(items),
                "items": items,
            }

            # 5) emit to SQS
            producer.send_json(payload)
            log.info("Sent %s items to SQS for %s", len(items), key)

            # 6) persist last run
            state.update(provider, branch, typ, ts_iso, key)
        except Exception as e:
            log.exception("Failed to process record: %s", e)

    return {"ok": True, "processed": len(records)}
