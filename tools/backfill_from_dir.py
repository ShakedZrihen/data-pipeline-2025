import os, json, time
from pathlib import Path
import boto3

OUTBOX_DIR = Path(r"extractor_lambda/outbox")
QUEUE_URL = os.environ.get("SQS_QUEUE_URL") or os.environ.get("OUTPUT_QUEUE_URL")
if not QUEUE_URL:
    raise SystemExit("Please set SQS_QUEUE_URL or OUTPUT_QUEUE_URL")

endpoint = os.environ.get("AWS_ENDPOINT_URL")
region = os.environ.get("AWS_REGION", "us-east-1")
access_key = os.environ.get("AWS_ACCESS_KEY_ID", "test")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")

sqs = boto3.client(
    "sqs",
    region_name=region,
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)

def normalize(msg: dict) -> dict:
    t = msg.get("type", "")
    msg["type"] = t.lower() if isinstance(t, str) else t
    if "ts" not in msg and "timestamp" in msg:
        msg["ts"] = msg["timestamp"]
    if "items" not in msg and "items_sample" in msg:
        msg["items"] = msg["items_sample"]
    for it in msg.get("items", []):
        u = it.get("unit")
        if u == "liter": it["unit"] = "l"
    return msg

def main():
    files = sorted(OUTBOX_DIR.glob("*.json"))
    print(f"Found {len(files)} files in {OUTBOX_DIR}")
    for i, p in enumerate(files, 1):
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                payloads = data
            else:
                payloads = [data]
            for obj in payloads:
                body = json.dumps(normalize(obj), ensure_ascii=False, separators=(",", ":"))
                sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=body)
        except Exception as e:
            print(f"[WARN] {p.name}: {e}")
        if i % 50 == 0:
            time.sleep(0.2)
    print("Backfill done.")

if __name__ == "__main__":
    main()
