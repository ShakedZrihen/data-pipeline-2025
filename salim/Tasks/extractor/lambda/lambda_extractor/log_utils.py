# extractor/log_utils.py
import os
import json
import datetime
import boto3
from botocore.config import Config

# ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://localhost:4566")
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://s3-simulator:4566")
AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")
LOG_BUCKET   = os.getenv("LOG_BUCKET", os.getenv("S3_BUCKET", "providers-bucket"))
LOG_TO_S3    = os.getenv("LOG_TO_S3", "0") == "1"

_s3 = None
def _s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            region_name=AWS_REGION,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            config=Config(s3={"addressing_style": "path"}),
        )
    return _s3

def log(message: str, **ctx):
    # Console (container stdout)
    ts = datetime.datetime.utcnow().isoformat() + "Z"
    line = {"ts": ts, "msg": message, **({} if not ctx else {"ctx": ctx})}
    print(json.dumps(line, ensure_ascii=False))
    # Optional S3 mirror
    if LOG_TO_S3:
        key = f"logs/extractor/{ts.replace(':','-')}_{os.getpid()}.jsonl"
        _s3_client().put_object(
            Bucket=LOG_BUCKET,
            Key=key,
            Body=(json.dumps(line, ensure_ascii=False) + "\n").encode("utf-8"),
            ContentType="application/json",
        )

def log_exception(prefix: str, exc: Exception, **ctx):
    log(f"{prefix}: {exc}", **ctx)
