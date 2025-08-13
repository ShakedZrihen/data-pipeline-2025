# extractor.py
from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

from parser import parse_to_items
from sqs_producer import send_envelope
from state_store import already_processed, update_last_processed
from local_saver import save_result_copy

logger = logging.getLogger()
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

QUEUE_URL = os.getenv("QUEUE_URL", "").strip()
DB_TABLE = os.getenv("DB_TABLE", "PriceExtractorState").strip()

KEY_RE = re.compile(
    r"^providers/(?P<provider>[^/]+)/(?P<branch>[^/]+)/(?P<type>pricesFull|promoFull)_(?P<ts>[^/]+)\.gz$"
)

def lambda_handler(event, context):
    logger.info("Event: %s", json.dumps(event, ensure_ascii=False))
    results: List[Dict[str, Any]] = []

    for rec in event.get("Records", []):
        try:
            bucket = rec["s3"]["bucket"]["name"]
            key = unquote_plus(rec["s3"]["object"]["key"])
            etag = rec["s3"]["object"].get("eTag") or rec["s3"]["object"].get("etag")

            meta = parse_key(key)
            if not meta:
                logger.warning("Skipping unexpected key path: %s", key)
                continue

            logger.info("Processing s3://%s/%s -> %s", bucket, key, meta)

            if already_processed(DB_TABLE, meta, key, etag):
                logger.info("Already processed %s (etag=%s); skipping.", key, etag)
                continue

            text = read_text_auto(bucket, key)
            if looks_like_html(text):
                logger.warning("Non-data (HTML) content for %s; skipping.", key)
                update_last_processed(DB_TABLE, meta, key, etag)
                results.append({"key": key, "sent": 0, "items": 0, "skipped": True})
                continue

            items = parse_to_items(text)
            logger.info("Extracted %d items from %s", len(items), key)

            envelope = {
                "provider": meta["provider"],
                "branch": meta["branch"],
                "type": meta["type"],
                "timestamp": meta["timestamp"],
                "items": items,
            }

            save_result_copy(envelope)
            sent = send_envelope(envelope, QUEUE_URL)
            logger.info("Dispatched %d SQS message(s) for %s", sent, key)

            update_last_processed(DB_TABLE, meta, key, etag)
            results.append({"key": key, "sent": sent, "items": len(items)})

        except Exception as e:
            logger.exception("Failed processing record: %s", e)

    return {"ok": True, "results": results}

def parse_key(key: str) -> Optional[Dict[str, str]]:
    m = KEY_RE.match(key)
    if not m:
        return None
    return {
        "provider": m.group("provider"),
        "branch": m.group("branch"),
        "type": m.group("type"),
        "timestamp": normalize_timestamp(m.group("ts")),
    }

def _plausible_ymdhm(s: str) -> bool:
    if not (s.isdigit() and len(s) == 12):  # YYYYMMDDHHMM
        return False
    y, mo, d, hh, mm = int(s[:4]), int(s[4:6]), int(s[6:8]), int(s[8:10]), int(s[10:12])
    return (2010 <= y <= 2099) and (1 <= mo <= 12) and (1 <= d <= 31) and (0 <= hh <= 23) and (0 <= mm <= 59)

def normalize_timestamp(ts: str) -> str:
    # Try compact if it looks valid
    try:
        if _plausible_ymdhm(ts):
            return datetime.strptime(ts, "%Y%m%d%H%M").replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Try a few common formats
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y%m%d-%H%M", "%Y-%m-%d_%H%M", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(ts, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue
        # Flexible ISO
        iso = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
        dt = datetime.fromisoformat(iso)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def read_text_auto(bucket: str, key: str) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    raw = gzip.decompress(body) if (len(body) >= 2 and body[:2] == b"\x1f\x8b") else body
    return try_decode(raw)

def try_decode(raw: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "windows-1255", "iso-8859-8"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")

def looks_like_html(text: str) -> bool:
    s = text.lstrip().lower()
    return s.startswith("<!doctype html") or s.startswith("<html") or "<html" in s[:2048]
