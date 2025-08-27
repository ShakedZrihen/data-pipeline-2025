import json
import os
import boto3
import config

import re
OUTPUT_JSON_PREFIX = os.getenv("OUTPUT_JSON_PREFIX", "Json/").strip()

# ensure trailing slash (so users can pass "Json" or "Json/")
if OUTPUT_JSON_PREFIX and not OUTPUT_JSON_PREFIX.endswith("/"):
    OUTPUT_JSON_PREFIX = OUTPUT_JSON_PREFIX + "/"

_s3 = boto3.client(
    "s3",
    endpoint_url=config.ENDPOINT_URL,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=config.AWS_REGION
)

def write_payload_json(payload: dict):
    """
    Writes payload as pretty JSON:
    s3://{S3_BUCKET}/{OUTPUT_JSON_PREFIX}{provider}/{branch}/{type}_{YYYYMMDDhhmmss}.json
    """
    provider = payload.get("provider", "unknown")
    branch = payload.get("branch", "unknown")
    safe_branch = re.sub(r"[^a-zA-Z0-9_\-]", "_", branch)
    file_type = payload.get("type", "unknown")
    ts_iso = payload.get("timestamp")  # e.g., 2025-08-06T18:00:00Z

    # turn ISO into compact yyyyMMddHHmmss for filename
    # payload['timestamp'] is Zulu ISO string
    safe_ts = ts_iso.replace("-", "").replace(":", "").replace("T", "").replace("Z", "") if ts_iso else "00000000000000"

    key = f"{OUTPUT_JSON_PREFIX}{provider}/{safe_branch}/{file_type}_{safe_ts}.json"

    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    _s3.put_object(Bucket=config.S3_BUCKET, Key=key, Body=body, ContentType="application/json; charset=utf-8")
    return key
