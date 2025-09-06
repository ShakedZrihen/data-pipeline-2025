from typing import Any, Dict

class ValidationError(Exception):
    pass

REQ_BASE = ["provider", "branch", "type", "timestamp"]

def validate_message(msg: Dict[str, Any]) -> None:
    missing = [k for k in REQ_BASE if k not in msg]
    if missing:
        raise ValidationError(f"Missing required fields: {missing}")

    if not isinstance(msg["provider"], str) or not msg["provider"]:
        raise ValidationError("provider must be non-empty string")
    if not isinstance(msg["branch"], (str, int)):
        raise ValidationError("branch must be str or int")
    if not isinstance(msg["type"], str) or not msg["type"]:
        raise ValidationError("type must be non-empty string")
    if not isinstance(msg["timestamp"], str) or not msg["timestamp"]:
        raise ValidationError("timestamp must be ISO8601 string")

    has_inline_items = ("items" in msg) or ("items_sample" in msg)
    has_s3_pointer = ("s3_bucket" in msg and "s3_key" in msg)

    if not (has_inline_items or has_s3_pointer):
        raise ValidationError("Missing items: provide items/items_sample OR s3_bucket+s3_key")

    if has_inline_items and "items_sample" in msg:
        if not isinstance(msg["items_sample"], list):
            raise ValidationError("items_sample must be a list")
        for i, it in enumerate(msg["items_sample"]):
            if not isinstance(it, dict):
                raise ValidationError(f"items_sample[{i}] must be an object")
            for k in ["product", "price", "unit"]:
                if k not in it:
                    raise ValidationError(f"items_sample[{i}] missing field '{k}'")
