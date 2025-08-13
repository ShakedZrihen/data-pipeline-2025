import json
import os
import boto3

ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "providers-bucket")
OUTPUT_JSON_PREFIX = os.getenv("OUTPUT_JSON_PREFIX", "Json/").strip()

# ensure trailing slash (so users can pass "Json" or "Json/")
if OUTPUT_JSON_PREFIX and not OUTPUT_JSON_PREFIX.endswith("/"):
    OUTPUT_JSON_PREFIX = OUTPUT_JSON_PREFIX + "/"

_s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

def write_payload_json(payload: dict):
    """
    Writes payload as pretty JSON:
    s3://{S3_BUCKET}/{OUTPUT_JSON_PREFIX}{provider}/{branch}/{type}_{YYYYMMDDhhmmss}.json
    """
    provider = payload.get("provider", "unknown")
    branch = payload.get("branch", "unknown")
    file_type = payload.get("type", "unknown")
    ts_iso = payload.get("timestamp")  # e.g., 2025-08-06T18:00:00Z

    # turn ISO into compact yyyyMMddHHmmss for filename
    # payload['timestamp'] is Zulu ISO string
    safe_ts = ts_iso.replace("-", "").replace(":", "").replace("T", "").replace("Z", "") if ts_iso else "00000000000000"

    key = f"{OUTPUT_JSON_PREFIX}{provider}/{branch}/{file_type}_{safe_ts}.json"

    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    _s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json; charset=utf-8")
    return key

def write_json_to_s3(payload):
    return write_payload_json(payload)
