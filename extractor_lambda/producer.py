import os, json, logging, boto3
from botocore.config import Config

log = logging.getLogger(__name__)

MAX_SQS_BYTES = 256 * 1024 - 2048

def _sqs():
    return boto3.client(
        "sqs",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(retries={'max_attempts': 3, 'mode': 'standard'})
    )

def _json_size_bytes(obj) -> int:
    return len(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))

def _shrink_to_bytes(obj: dict, max_bytes: int) -> dict:

    def _trim_text(s: str, lim: int) -> str:
        if not isinstance(s, str) or len(s) <= lim:
            return s
        t = s[:lim]
        while t and '\u0590' <= t[-1] <= '\u05FF':
            t = t[:-1]
        return t + "…"

    if _json_size_bytes(obj) <= max_bytes:
        return obj

    items = list(obj.get("items_sample", []))
    trim_limit = 160

    while True:
        for it in items:
            for k in ("product", "promo_text", "brand", "category"):
                if k in it and isinstance(it[k], str):
                    it[k] = _trim_text(it[k], trim_limit)

        tmp = dict(obj, items_sample=items)
        if _json_size_bytes(tmp) <= max_bytes:
            return tmp

        if len(items) > 10:
            new_len = max(10, int(len(items) * 0.9))
            if new_len == len(items):
                new_len = len(items) - 1
            items = items[:new_len]
        else:
            if trim_limit > 60:
                trim_limit = int(trim_limit * 0.9)
            else:
                obj["items_sample"] = items[:10]
                return obj

def send_message(payload: dict, outbox_path: str | None = None):

    queue_url = os.getenv("OUTPUT_QUEUE_URL")
    if not queue_url:
        raise ValueError("Missing OUTPUT_QUEUE_URL")

    items = list(payload.get("items", []))

    enriched_payload = dict(payload)
    enriched_payload["items_total"] = len(items)
    enriched_payload["items_sample"] = items[:10]

    if outbox_path:
        enriched_payload["outbox_path"] = outbox_path

    safe_payload = _shrink_to_bytes(enriched_payload, MAX_SQS_BYTES)

    try:
        with open("items_sample.json", "w", encoding="utf-8") as f:
            json.dump(safe_payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("Failed writing items_sample.json: %s", e)

    sqs = _sqs()
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(safe_payload, ensure_ascii=False))
    log.info("SQS send ok: provider=%s branch=%s type=%s total=%d",
             safe_payload.get("provider"), safe_payload.get("branch"),
             safe_payload.get("type"), safe_payload.get("items_total", 0))
