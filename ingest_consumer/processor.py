import json
import logging
from .validator import validate_message, ValidationError
from .normalizer import normalize
log = logging.getLogger(__name__)

class ProcessingError(Exception):
    pass

def process_raw_message(body_str: str) -> dict:

    try:
        msg = json.loads(body_str)
    except Exception as e:
        raise ProcessingError(f"Body is not JSON: {e}")
    try:
        validate_message(msg)
        msg = normalize(msg)
        return msg
    except ValidationError as e:
        raise ProcessingError(str(e))
