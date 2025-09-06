# consumer/log.py
from __future__ import annotations
import json
import logging
import sys
from typing import Any, Dict

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            payload.update(extra)
        return json.dumps(payload, ensure_ascii=False)

def setup_logging(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("consumer")
    logger.setLevel(level)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    h = logging.StreamHandler(stream=sys.stdout)
    h.setFormatter(JsonFormatter())
    logger.addHandler(h)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    return logger
