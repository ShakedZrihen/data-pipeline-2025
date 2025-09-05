from __future__ import annotations
import os, json, uuid, re
from pathlib import Path
from typing import Any, Dict, Optional

SAFE_RE = re.compile(r"[^A-Za-z0-9._-]+")

def _slug(s: Optional[str]) -> str:
    return SAFE_RE.sub("_", s or "unknown").strip("_") or "unknown"

def _safe_ts(s: Optional[str]) -> str:
    # 2025-09-01T12:34:56Z -> 20250901_123456Z
    if not s:
        return "na"
    return s.replace("-", "").replace(":", "").replace("T", "_")

class InboxSink:
    """
    Writes messages once to /out/inbox (configurable via OUT_DIR).
    We treat these as ephemeral 'inbox' files consumed by the aggregator.
    """
    def __init__(self, out_dir: Optional[str] = None, subdir: str = "inbox"):
        base = Path(out_dir or os.getenv("OUT_DIR", "/out"))
        self.dir = base / subdir
        self.dir.mkdir(parents=True, exist_ok=True)

    def write(self, payload: Dict[str, Any]) -> str:
        prov   = _slug(payload.get("provider"))
        branch = _slug(payload.get("branch"))
        typ    = _slug(payload.get("type"))
        ts     = _safe_ts(payload.get("timestamp"))
        leaf   = f"{prov}_{branch}_{typ}_{ts}_{uuid.uuid4().hex[:8]}.json"
        path   = self.dir / leaf

        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)  # single write
        tmp.replace(path)  # atomic-ish rename
        return str(path)

    @staticmethod
    def remove(path: str) -> None:
        try:
            Path(path).unlink(missing_ok=True)  # py3.8+
        except Exception:
            pass
