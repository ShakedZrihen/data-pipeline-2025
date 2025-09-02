import os
import re
import json
import time
import pathlib
from typing import Any, Dict, List, Optional

def _slug(s: Optional[str]) -> str:
    s = (s or "").strip()
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)[:100].strip("_")

class Aggregator:
    """
    Groups incoming payloads by (provider, branch, type, timestamp|source_key) and merges their items.
    Flushes when all parts are received or when a group is stale beyond flush_age_s.
    """

    def __init__(self, out_dir: Optional[str] = None, flush_age_s: int = 60):
        self.groups: Dict[str, Dict[str, Any]] = {}
        self.out_dir = out_dir or os.getenv("OUT_DIR", "/app/enricher/out")
        self.flush_age_s = flush_age_s
        pathlib.Path(self.out_dir).mkdir(parents=True, exist_ok=True)

    def _group_key(self, p: Dict[str, Any]) -> str:
        # Prefer timestamp for safety (avoid merging different runs); fall back to source_key; finally to just triplet.
        provider = p.get("provider", "")
        branch   = p.get("branch", "")
        typ      = p.get("type", "")
        ts       = p.get("timestamp") or ""
        src      = p.get("source_key") or ""
        if ts:
            return f"{provider}#{branch}#{typ}#{ts}"
        if src:
            return f"{provider}#{branch}#{typ}#{src}"
        return f"{provider}#{branch}#{typ}"

    def add(self, payload: Dict[str, Any]) -> Optional[str]:
        """
        Add one payload (possibly a 'part' of a larger set).
        Returns the output path when the group is flushed (None otherwise).
        """
        k = self._group_key(payload)
        g = self.groups.get(k)
        if not g:
            g = {
                "provider": payload.get("provider", ""),
                "branch": payload.get("branch", ""),
                "type": payload.get("type", ""),
                "timestamp": payload.get("timestamp") or "",
                "source_key": payload.get("source_key", ""),
                "items": [],
                "parts_expected": int(payload["parts"]) if str(payload.get("parts", "")).isdigit() else None,
                "parts_seen": set(),   # {1,2,3,...}
                "first_seen": time.time(),
            }
            self.groups[k] = g

        # merge items
        items = payload.get("items") or []
        if isinstance(items, list):
            g["items"].extend(items)

        # parts bookkeeping (optional)
        part = payload.get("part")
        if str(part).isdigit():
            g["parts_seen"].add(int(part))
        if g["parts_expected"] is None and str(payload.get("parts", "")).isdigit():
            g["parts_expected"] = int(payload["parts"])

        # flush immediately if we know we've got everything
        if g["parts_expected"] and len(g["parts_seen"]) >= g["parts_expected"]:
            return self._flush_key(k)

        return None

    def flush_stale(self) -> List[str]:
        """Flush groups older than flush_age_s, even if not all parts were announced."""
        now = time.time()
        out_paths: List[str] = []
        for k, g in list(self.groups.items()):
            if now - g["first_seen"] >= self.flush_age_s:
                p = self._flush_key(k)
                if p:
                    out_paths.append(p)
        return out_paths

    def _flush_key(self, k: str) -> Optional[str]:
        g = self.groups.pop(k, None)
        if not g:
            return None

        dedup: Dict[str, Any] = {}
        others: List[Dict[str, Any]] = []

        is_promo = (g["type"] or "").lower() == "promofull"

        for it in g["items"]:
            if not isinstance(it, dict):
                others.append(it)
                continue
            code = it.get("code")
            if code is None:
                others.append(it)
                continue
            if is_promo:
                promo_id = it.get("promotion_id") or ""
                dkey = f"{code}|{promo_id}"
            else:
                dkey = str(code)
            dedup[dkey] = it

        merged_items = list(dedup.values()) + others

        doc = {
            "provider": g["provider"],
            "branch": g["branch"],
            "type": g["type"],
            "timestamp": g["timestamp"],
            "source_key": g["source_key"],
            "item_count": len(merged_items),
            "items": merged_items,
        }

        # out path: /OUT_DIR/<provider>/<branch>/<type>_<timestamp>.json
        provider_s = _slug(g["provider"])
        branch_s   = _slug(g["branch"])
        type_s     = _slug(g["type"])
        ts_s       = _slug(g["timestamp"]) or "na"

        dirpath = pathlib.Path(self.out_dir) / provider_s / branch_s
        dirpath.mkdir(parents=True, exist_ok=True)
        out_path = dirpath / f"{type_s}_{ts_s}.json"

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False)

        return str(out_path)
