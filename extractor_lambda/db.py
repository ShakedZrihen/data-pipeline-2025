import os
import time
import logging
import boto3
from botocore.config import Config

log = logging.getLogger("extractor")

def _client(service: str):
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    region = os.getenv("AWS_REGION", "us-east-1")
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client(service, region_name=region, endpoint_url=endpoint, config=cfg)

def upsert_last_run(provider: str, branch: str, type_: str, iso_ts: str):
    if os.getenv("ENABLE_DB", "").lower() not in ("1", "true", "yes"):
        log.warning("[local] DB disabled (ENABLE_DB not set) — skipping upsert")
        return None

    table_name = os.getenv("LAST_RUNS_TABLE") or os.getenv("LAST_RUN_TABLE") or "last_runs"
    ddb = _client("dynamodb")

    pk = f"{provider}#{branch}#{type_}"
    now_epoch = int(time.time())
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    ddb.put_item(
        TableName=table_name,
        Item={
            "pk": {"S": pk},
            "last_run_iso": {"S": iso_ts},
            "updated_at": {"N": str(now_epoch)},
            "updated_at_iso": {"S": now_iso}
        }
    )
    log.info(f"last_runs upsert: table={table_name} pk={pk} ts={iso_ts}")
    return pk

