import os, json, time, gzip, io, decimal
import boto3
import xml.etree.ElementTree as ET
from urllib.parse import unquote
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError

AWS_REGION    = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ENDPOINT      = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
S3_ENDPOINT   = os.getenv("S3_ENDPOINT", ENDPOINT)
SQS_ENDPOINT  = os.getenv("SQS_ENDPOINT", ENDPOINT)
DDB_ENDPOINT  = os.getenv("DDB_ENDPOINT", ENDPOINT)

IN_QUEUE      = os.getenv("IN_QUEUE",  "price-ingest")
OUT_QUEUE     = os.getenv("OUT_QUEUE", "price-events")
DDB_TABLE     = os.getenv("DDB_TABLE", "last_run")
MAX_ITEMS_PER_MESSAGE = int(os.getenv("MAX_ITEMS_PER_MESSAGE", "250"))

cfg = Config(s3={"addressing_style": "path"})
s3  = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name=AWS_REGION, config=cfg,aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
sqs = boto3.client("sqs", endpoint_url=SQS_ENDPOINT, region_name=AWS_REGION,aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
ddb = boto3.resource("dynamodb", endpoint_url=DDB_ENDPOINT, region_name=AWS_REGION,aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
table = ddb.Table(DDB_TABLE)

def qurl(name: str) -> str:
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]

def gunzip_to_text(blob: bytes) -> str:
    data = gzip.decompress(blob)
    for enc in ("utf-8", "cp1255", "iso-8859-8"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")

def safe_float(x):
    if x is None:
        return None
    s = str(x).strip().replace(",", ".")
    try:
        return float(decimal.Decimal(s))
    except Exception:
        return None

def derive_unit(fields: dict) -> str | None:
    uom = fields.get("UnitOfMeasure")
    if uom:
        return uom.strip()
    qty = fields.get("Quantity")
    qty_unit = fields.get("UnitQty")
    if qty and qty_unit:
        return f"{str(qty).strip()} {str(qty_unit).strip()}"
    return None

def iter_items_xml(xml_text: str):
    f = io.StringIO(xml_text)
    for event, elem in ET.iterparse(f, events=("end",)):
        if elem.tag.endswith("Item"):
            row = {}
            for child in list(elem):
                tag = child.tag.split("}")[-1]
                row[tag] = (child.text or "").strip()
            name  = row.get("ItemName") or row.get("ManufacturerItemDescription") or row.get("Description")
            price = safe_float(row.get("ItemPrice") or row.get("Price"))
            unit  = derive_unit(row)

            if name and price is not None:
                yield {"product": name, "price": price, "unit": unit}
            elem.clear()

def chunked(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def update_last_run(provider: str, branch: str, typ: str, ts_iso: str, key: str, etag: str | None):
    pk = f"{provider}#{branch}#{typ}"
    table.put_item(Item={
        "pk": pk,
        "lastFileTimestamp": ts_iso,
        "lastProcessedKey": key,
        "etag": etag or "",
        "updatedAt": datetime.now(timezone.utc).isoformat()
    })

def should_skip(provider: str, branch: str, typ: str, key: str, etag: str | None) -> bool:
    pk = f"{provider}#{branch}#{typ}"
    resp = table.get_item(Key={"pk": pk})
    item = resp.get("Item")
    if not item:
        return False
    return (item.get("lastProcessedKey") == key) or (etag and item.get("etag") == etag)

def process_job(job: dict, out_q_url: str):
    bucket = job["bucket"]
    key    = job["key"]
    provider = job["provider"]
    branch   = job["branch"]
    typ      = job["type"]
    ts_iso   = job["timestamp"]

    head = s3.head_object(Bucket=bucket, Key=unquote(key))
    etag = head.get("ETag", "").strip('"')
    if should_skip(provider, branch, typ, key, etag):
        print(f"[SKIP] Already processed: {key}")
        return 0

    obj = s3.get_object(Bucket=bucket, Key=unquote(key))
    text = gunzip_to_text(obj["Body"].read())
    items_iter = iter_items_xml(text)

    base_doc = {
        "provider": provider,
        "branch": branch,
        "type": typ,
        "timestamp": ts_iso,
    }
    sent = 0
    for part in chunked(items_iter, MAX_ITEMS_PER_MESSAGE):
        payload = dict(base_doc)
        payload["items"] = part
        sqs.send_message(QueueUrl=out_q_url, MessageBody=json.dumps(payload, ensure_ascii=False))
        sent += len(part)

    update_last_run(provider, branch, typ, ts_iso, key, etag)
    print(f"[OK] {key} -> {sent} items")
    return sent

def main_loop():
    in_q_url  = qurl(IN_QUEUE)
    out_q_url = qurl(OUT_QUEUE)
    print(f"Worker running. IN={in_q_url} OUT={out_q_url}")

    while True:
        resp = sqs.receive_message(
            QueueUrl=in_q_url,
            MaxNumberOfMessages=5,
            WaitTimeSeconds=10,
            VisibilityTimeout=60
        )
        if "Messages" not in resp:
            continue

        for m in resp["Messages"]:
            try:
                job = json.loads(m["Body"])
                process_job(job, out_q_url)
                sqs.delete_message(QueueUrl=in_q_url, ReceiptHandle=m["ReceiptHandle"])
            except Exception as e:
                print("ERROR:", e)
        time.sleep(0.1)

if __name__ == "__main__":
    main_loop()
