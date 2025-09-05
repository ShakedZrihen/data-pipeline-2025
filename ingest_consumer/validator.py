from typing import Any, Dict, List

class ValidationError(Exception):
    pass

REQUIRED_TOP = ["provider", "branch", "type", "timestamp", "items_total", "items_sample"]

def validate_message(msg: Dict[str, Any]) -> None:
    missing = [k for k in REQUIRED_TOP if k not in msg]
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
    if not isinstance(msg["items_total"], int) or msg["items_total"] < 0:
        raise ValidationError("items_total must be non-negative int")
    if not isinstance(msg["items_sample"], list):
        raise ValidationError("items_sample must be a list")

    for i, it in enumerate(msg["items_sample"]):
        if not isinstance(it, dict):
            raise ValidationError(f"items_sample[{i}] must be an object")
        for k in ["product", "price", "unit"]:
            if k not in it:
                raise ValidationError(f"items_sample[{i}] missing field '{k}'")
        if not isinstance(it["product"], str) or not it["product"].strip():
            raise ValidationError(f"items_sample[{i}].product must be non-empty string")
        if not isinstance(it["price"], (int, float)):
            raise ValidationError(f"items_sample[{i}].price must be number")
        if not isinstance(it["unit"], str) or not it["unit"].strip():
            raise ValidationError(f"items_sample[{i}].unit must be non-empty string")
