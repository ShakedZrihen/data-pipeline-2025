import os, json, boto3, logging
from .validator import validate_message, ValidationError
_s3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
)

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
def load_items_from_pointer(msg: dict) -> list:
    if not (msg.get("s3_bucket") and msg.get("s3_key")):
        return []
    s3 = _s3()
    bkt, key = msg["s3_bucket"], msg["s3_key"]
    logging.info(f"S3 GET: s3://{bkt}/{key}")
    obj = s3.get_object(Bucket=bkt, Key=key)
    data = json.loads(obj["Body"].read().decode("utf-8", errors="ignore"))
    items = data.get("items") or data.get("items_sample") or []
    items_total = data.get("items_total", len(items))
    if isinstance(data, dict) and "items" in data:
        items = data["items"]
    elif isinstance(data, list):
        items = data
    else:
        items = []
    logging.info(f"S3 LOADED {len(items)} items from pointer")
    return items,items_total

def process_raw_message(body_str: str) -> dict:
    msg = json.loads(body_str)
    try:
        msg = json.loads(body_str)
        validate_message(msg)

        if "s3_bucket" in msg and "s3_key" in msg:
            items, items_total = load_items_from_pointer(msg)
            msg["items"] = items
            msg.setdefault("items_total", items_total)
        else:
            if "items" not in msg and "items_sample" in msg:
                msg["items"] = msg.pop("items_sample")
            msg.setdefault("items_total", len(msg.get("items", [])))
        items = []
        if isinstance(msg.get("items"), list):
            items = msg["items"]
        elif msg.get("s3_bucket") and msg.get("s3_key"):
            items, _ = load_items_from_pointer(msg)
        msg["items"] = items
        msg["items_total"] = len(items)
        msg["items_sample"] = items[:3] if items else []
        return msg
    except ValidationError as e:
        raise ProcessingError(str(e))
    except Exception as e:
        raise ProcessingError(f"Unexpected error: {e}")
