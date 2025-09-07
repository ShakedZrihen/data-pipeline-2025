import os, json, boto3, logging
from .validator import validate_message, ValidationError

class ProcessingError(Exception):
    pass

def _s3():
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )

def load_items_from_pointer(msg: dict) -> tuple[list, int]:
    if not (msg.get("s3_bucket") and msg.get("s3_key")):
        return [], 0
    s3 = _s3()
    bkt, key = msg["s3_bucket"], msg["s3_key"]
    logging.info(f"S3 GET: s3://{bkt}/{key}")
    obj = s3.get_object(Bucket=bkt, Key=key)
    data = json.loads(obj["Body"].read().decode("utf-8", errors="ignore"))
    items = []
    if isinstance(data, dict) and "items" in data:
        items = data["items"] or data.get("items_sample") or []
        items_total = int(data.get("items_total") or len(items))
    elif isinstance(data, list):
        items = data
        items_total = len(items)
    else:
        items_total = 0
    logging.info(f"S3 LOADED {len(items)} items from pointer")
    return items, items_total

def process_raw_message(body_str: str) -> dict:
    body_str = body_str.lstrip("\ufeff").strip()
    try:
        msg = json.loads(body_str)
    except json.JSONDecodeError as e:
        raise ProcessingError(f"Invalid JSON: {e.msg}")

    try:
        validate_message(msg)

        if msg.get("s3_bucket") and msg.get("s3_key"):
            items, items_total = load_items_from_pointer(msg)
            msg["items"] = items
            msg.setdefault("items_total", items_total)
        else:
            if "items" not in msg and "items_sample" in msg:
                msg["items"] = msg.pop("items_sample")
            msg.setdefault("items_total", len(msg.get("items", [])))

        items = msg.get("items") or []
        msg["items"] = items
        msg["items_total"] = int(msg.get("items_total") or len(items))
        msg["items_sample"] = items[:3] if items else []

        return msg

    except ValidationError as e:
        raise ProcessingError(str(e))
    except Exception as e:
        raise ProcessingError(f"Unexpected error: {e}")
