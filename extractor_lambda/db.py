
import os
import time
import logging
import boto3
from botocore.config import Config
from typing import Optional, Dict, Any

log = logging.getLogger("extractor")


def _client(service: str):
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    region = os.getenv("AWS_REGION", "us-east-1")
    cfg = Config(retries={"max_attempts": 3, "mode": "standard"})
    return boto3.client(
        service,
        region_name=region,
        endpoint_url=endpoint,
        config=cfg,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )


def _table_name() -> str:
    return os.getenv("LAST_RUNS_TABLE") or os.getenv("LAST_RUN_TABLE") or "last_runs"


def upsert_last_run(
    provider: str,
    branch: str,
    type_: str,
    iso_ts: str,
    *,
    s3_bucket: Optional[str] = None,
    s3_key: Optional[str] = None,
    items_total: Optional[int] = None,
    items_sample_count: Optional[int] = None,
) -> str | None:
    if os.getenv("ENABLE_DB", "").lower() not in ("1", "true", "yes"):
        log.warning("[local] DB disabled (ENABLE_DB not set) — skipping upsert")
        return None

    ddb = _client("dynamodb")
    table_name = _table_name()

    pk = f"{provider}#{branch}#{type_}"
    now_epoch = int(time.time())
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    item: Dict[str, Dict[str, Any]] = {
        "pk": {"S": pk},
        "provider": {"S": provider},
        "branch": {"S": branch},
        "type": {"S": type_},
        "last_run_iso": {"S": iso_ts},
        "updated_at": {"N": str(now_epoch)},
        "updated_at_iso": {"S": now_iso},
    }

    if s3_bucket:
        item["s3_bucket"] = {"S": s3_bucket}
    if s3_key:
        item["s3_key"] = {"S": s3_key}
    if items_total is not None:
        item["items_total"] = {"N": str(int(items_total))}
    if items_sample_count is not None:
        item["items_sample_count"] = {"N": str(int(items_sample_count))}

    ddb.put_item(TableName=table_name, Item=item)
    log.info(
        "last_runs upsert: table=%s pk=%s ts=%s bucket=%s key=%s items_total=%s sample=%s",
        table_name, pk, iso_ts, s3_bucket, s3_key, items_total, items_sample_count
    )
    return pk


def get_last_run(provider: str, branch: str, type_: str) -> Optional[Dict[str, Any]]:
    if os.getenv("ENABLE_DB", "").lower() not in ("1", "true", "yes"):
        return None
    ddb = _client("dynamodb")
    table_name = _table_name()
    pk = f"{provider}#{branch}#{type_}"

    resp = ddb.get_item(TableName=table_name, Key={"pk": {"S": pk}})
    return resp.get("Item")


def list_last_runs(limit: int = 50) -> list[Dict[str, Any]]:
    if os.getenv("ENABLE_DB", "").lower() not in ("1", "true", "yes"):
        return []
    ddb = _client("dynamodb")
    table_name = _table_name()
    resp = ddb.scan(TableName=table_name, Limit=limit)
    return resp.get("Items", [])


