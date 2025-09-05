import json
import logging

log = logging.getLogger(__name__)

class ProcessingError(Exception):
    pass

def process_raw_message(body_str: str) -> dict:

    try:
        msg = json.loads(body_str)
    except Exception as e:
        raise ProcessingError(f"Body is not JSON: {e}")

    required = ["provider", "branch", "type", "timestamp"]
    missing = [k for k in required if k not in msg]
    if missing:
        raise ProcessingError(f"Missing required fields: {missing}")

    return msg
