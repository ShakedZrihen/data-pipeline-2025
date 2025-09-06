import os, json, re, time
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from ingest_consumer.db import upsert_supermarket
DB_PATH = os.environ.get("DB_PATH") or "data/prices.db"
QUEUE_URL = os.environ.get("SQS_QUEUE_URL") or os.environ.get("OUTPUT_QUEUE_URL")
AWS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
S3_BUCKET = os.environ.get("S3_BUCKET", "prices-backfill")

if not QUEUE_URL:
    raise SystemExit("Missing SQS_QUEUE_URL/OUTPUT_QUEUE_URL")

sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION,
    endpoint_url=AWS_ENDPOINT,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
)
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    endpoint_url=AWS_ENDPOINT,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
)

def ensure_bucket(bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)

def parse_from_filename(fname: str):
    m = re.match(r"^([^_]+)_([^_]+)_([^_]+)_(\d{8}_\d{6})\.json$", fname)
    if not m:
        return {"provider":"unknown","branch":"0","type":"unknown","timestamp":"1970-01-01T00:00:00Z"}
    provider, branch, typ, ts = m.groups()
    iso = f"{ts[0:4]}-{ts[4:6]}-{ts[6:8]}T{ts[9:11]}:{ts[11:13]}:{ts[13:15]}Z"
    upsert_supermarket(DB_PATH, provider, branch, fname)

    return {"provider":provider, "branch":branch, "type":typ, "timestamp":iso}

def main():
    base = Path("extractor_lambda/outbox")
    files = sorted([p for p in base.glob("*.json") if p.is_file()])
    print(f"QUEUE: {QUEUE_URL}")
    print(f"S3   : bucket={S3_BUCKET}, endpoint={AWS_ENDPOINT}")
    print(f"Found {len(files)} files in {base}")

    ensure_bucket(S3_BUCKET)

    for p in files:
        key = f"backfill/{p.name}"
        with open(p, "rb") as f:
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=f)

        meta = parse_from_filename(p.name)
        body = {
            **meta,
            "items_total": 0,
            "items_sample": [],
            "s3_bucket": S3_BUCKET,
            "s3_key": key,
        }
        body_str = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        if len(body_str.encode("utf-8")) >= 250_000:
            raise SystemExit(f"Message too large unexpectedly for {p.name}")

        resp = sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=body_str)
        print(f"OK: {p.name} -> {resp['MessageId']}")
        time.sleep(0.05)
if __name__ == "__main__":
    main()
