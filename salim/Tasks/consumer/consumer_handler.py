# # consumer/consumer_handler.py
# import json

# def handler(event, context):
#     for rec in event.get("Records", []):
#         body = rec.get("body", "")
#         try:
#             data = json.loads(body)
#         except Exception:
#             data = {"raw": body}
#         print("=== MESSAGE ===")
#         print(json.dumps(data, ensure_ascii=False, indent=2))
#     return {"ok": True}

# consumer/consumer_handler.py
import os
import json
import datetime
import boto3
from botocore.config import Config

ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://localhost:4566")
AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")
LOG_BUCKET   = os.getenv("LOG_BUCKET", os.getenv("S3_BUCKET", "providers-bucket"))
LOG_TO_S3    = os.getenv("LOG_TO_S3", "0") == "1"

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(s3={"addressing_style": "path"}),
    )

def _log_to_s3(obj):
    if not LOG_TO_S3: 
        return
    ts = datetime.datetime.utcnow().isoformat() + "Z"
    key = f"logs/consumer/{ts.replace(':','-')}_{os.getpid()}.jsonl"
    _s3().put_object(
        Bucket=LOG_BUCKET,
        Key=key,
        Body=(json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"),
        ContentType="application/json",
    )

def handler(event, context):
    count = 0
    for rec in event.get("Records", []):
        body = rec.get("body", "")
        try:
            data = json.loads(body)
        except Exception:
            data = {"raw": body}
        msg = {
            "ts": datetime.datetime.utcnow().isoformat() + "Z",
            "msg": "consumer_received",
            "len_items": len(data.get("items", [])) if isinstance(data, dict) else None,
            "provider": data.get("provider"),
            "branch": data.get("branch"),
            "type": data.get("type"),
            "timestamp": data.get("timestamp"),
        }
        print(json.dumps(msg, ensure_ascii=False))
        _log_to_s3(msg)
        count += 1
    print(json.dumps({"msg": "consumer_done", "processed": count}))
    _log_to_s3({"msg": "consumer_done", "processed": count})
    return {"ok": True, "processed": count}
