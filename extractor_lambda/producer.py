import os
import json
import logging
import boto3
from botocore.config import Config

log = logging.getLogger(__name__)

def _sqs():
    region = os.getenv("AWS_REGION", "us-east-1")
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client("sqs", region_name=region, endpoint_url=endpoint, config=cfg)

def _json_size_bytes(obj) -> int:
    return len(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))

def _shrink_to_bytes(obj: dict, max_bytes: int) -> dict:
    """
    sqs
    """
    if _json_size_bytes(obj) <= max_bytes:
        return obj

    items = list(obj.get("items_sample", []))
    trunc = 160
    while items:
        for it in items:
            name = it.get("product", "")
            if isinstance(name, str) and len(name) > trunc:
                it["product"] = name[:trunc] + "…"

        tmp = dict(obj, items_sample=items)
        if _json_size_bytes(tmp) <= max_bytes:
            return tmp

        new_len = max(10, int(len(items) * 0.9))
        if new_len == len(items):
            new_len = len(items) - 1
        items = items[:new_len]

        if trunc > 60:
            trunc = int(trunc * 0.9)

    obj["items_sample"] = []
    return obj

def send_message(payload: dict):
    if os.getenv("ENABLE_QUEUE", "1") != "1":
        log.info("[producer] queue disabled (ENABLE_QUEUE!=1)")
        return

    url = os.getenv("OUTPUT_QUEUE_URL")
    if not url:
        log.warning("[producer] OUTPUT_QUEUE_URL is empty; skipping send")
        return

    items = payload.get("items", [])
    sample_n = int(os.getenv("SQS_ITEMS_SAMPLE", "150"))
    body = {
        "provider": payload["provider"],
        "branch": payload["branch"],
        "type": payload["type"],
        "timestamp": payload["timestamp"],
        "items_total": len(items),
        "items_sample": items[:sample_n],
    }

    max_bytes = int(os.getenv("MAX_SQS_BYTES", "250000"))
    body = _shrink_to_bytes(body, max_bytes)

    s = json.dumps(body, ensure_ascii=False, separators=(",", ":"))

    sqs = _sqs()
    sqs.send_message(QueueUrl=url, MessageBody=s)
    log.info("[producer] sent to SQS: %s", url)
