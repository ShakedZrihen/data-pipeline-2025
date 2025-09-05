from typing import Any, Dict, List

class ValidationError(Exception):
    pass

_ALLOWED_TYPES = {"pricesFull"}

def _require(d: Dict[str, Any], key: str):
    if key not in d:
        raise ValidationError(f"Missing required field: {key}")

def _is_str(x) -> bool:
    return isinstance(x, str)

def _is_list(x) -> bool:
    return isinstance(x, list)

def _is_number(x) -> bool:
    return isinstance(x, (int, float))

def validate_message(msg: Dict[str, Any]) -> None:
    for k in ["provider", "branch", "type", "timestamp"]:
        _require(msg, k)

    if not _is_str(msg["provider"]) or not msg["provider"]:
        raise ValidationError("provider must be non-empty string")

    if not (_is_str(msg["branch"]) or isinstance(msg["branch"], int)):
        raise ValidationError("branch must be str or int")

    if msg["type"] not in _ALLOWED_TYPES:
        raise ValidationError(f"type must be one of: {_ALLOWED_TYPES}")

    if not _is_str(msg["timestamp"]) or not msg["timestamp"]:
        raise ValidationError("timestamp must be ISO8601 string")

    if "items_total" in msg and not isinstance(msg["items_total"], int):
        raise ValidationError("items_total must be int")

    if "items_sample" in msg:
        if not _is_list(msg["items_sample"]):
            raise ValidationError("items_sample must be a list")
        for i, item in enumerate(msg["items_sample"], start=1):
            if not isinstance(item, dict):
                raise ValidationError(f"items_sample[{i}] must be an object")
            for k in ["product", "price", "unit"]:
                _require(item, k)
            if not _is_str(item["product"]) or not item["product"]:
                raise ValidationError(f"items_sample[{i}].product must be non-empty string")
            if not _is_number(item["price"]) or item["price"] < 0:
                raise ValidationError(f"items_sample[{i}].price must be non-negative number")
            if not _is_str(item["unit"]) or not item["unit"]:
                raise ValidationError(f"items_sample[{i}].unit must be non-empty string")
