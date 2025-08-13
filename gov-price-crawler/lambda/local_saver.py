# local_saver.py
import json
import os
from pathlib import Path
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)

def _running_in_lambda() -> bool:
    return "AWS_LAMBDA_FUNCTION_NAME" in os.environ

def _compact_ts(iso_ts: str) -> str:
    # "2025-08-06T18:00:00Z" -> "20250806T180000Z"
    return iso_ts.replace("-", "").replace(":", "").replace(" ", "T")

def save_result_copy(envelope: Dict[str, Any]) -> None:
    """
    Save envelope JSON locally for review:
      - in Lambda: /tmp/results/<provider>/<branch>/<type>_<timestamp>.json
      - local run: ./results/<provider>/<branch>/<type>_<timestamp>.json
    """
    base_dir = Path("/tmp") if _running_in_lambda() else Path(".")
    out_dir = base_dir / "results" / envelope["provider"] / envelope["branch"]
    fname = f"{envelope['type']}_{_compact_ts(envelope['timestamp'])}.json"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        with open(out_dir / fname, "w", encoding="utf-8") as f:
            json.dump(envelope, f, ensure_ascii=False, indent=2)
        logger.info("Saved local result JSON: %s", str(out_dir / fname))
    except Exception as e:
        logger.warning("Failed writing local copy: %s", e)
