# consumer/validator.py
from __future__ import annotations
from typing import Any, Dict, Tuple, List

class MessageInvalid(Exception):
    pass

def is_s3_pointer(d: Dict[str, Any]) -> Tuple[bool, str | None, str | None]:
    if "bucket" in d and "key" in d:
        return True, d["bucket"], d["key"]
    s3 = d.get("s3")
    if isinstance(s3, dict) and "bucket" in s3 and "key" in s3:
        return True, s3["bucket"], s3["key"]
    return False, None, None

def is_items_payload(d: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]] | None]:
    items = d.get("items")
    if isinstance(items, list):
        return True, items
    return False, None

def validate_message(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accept either:
      • {"items": [.]}  OR
      • {"bucket": ".", "key": "."}  (or {"s3": {"bucket": ".", "key": "."}})
    """
    ok_items, items = is_items_payload(d)
    ok_s3, bucket, key = is_s3_pointer(d)

    if ok_items:
        return {"kind": "items", "items": items}
    if ok_s3:
        return {"kind": "s3", "bucket": bucket, "key": key}

    raise MessageInvalid("Payload must contain either 'items' or an S3 pointer {bucket,key}.")
