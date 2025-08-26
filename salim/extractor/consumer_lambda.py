# consumer_lambda.py
import io
import json
import gzip
import logging
import os
from typing import Any, Dict, Optional, Tuple, List

import boto3
from botocore.exceptions import ClientError

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# Reuse client across invocations
s3 = boto3.client("s3")

# Keys we try for previews inside dict-shaped JSON
PREVIEW_KEYS: Tuple[str, ...] = (
    "items", "products", "data", "promotions", "entries", "rows", "records", "results"
)


def _env() -> Tuple[str, str]:
    bucket = os.getenv("ITEMS_BUCKET")
    if not bucket:
        # Fail fast with a clear message instead of KeyError
        raise RuntimeError("Env var ITEMS_BUCKET is required")
    prefix = (os.getenv("PROCESSED_PREFIX") or "").strip("/")
    return bucket, prefix


def _from_s3_event(event: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    try:
        if "Records" in event:
            r0 = event["Records"][0]
            if r0.get("eventSource") == "aws:s3" and "s3" in r0:
                return r0["s3"]["bucket"]["name"], r0["s3"]["object"]["key"]
    except Exception:
        logger.exception("Failed to parse S3 event")
    return None


def _from_sqs_event(event: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    # Accept plain payloads or S3/SNS-enveloped payloads
    try:
        if "Records" in event and event["Records"]:
            r0 = event["Records"][0]
            if r0.get("eventSource") == "aws:sqs":
                body = r0.get("body") or ""
                payload = json.loads(body)
                # Case 1: direct {bucket,key} or {s3_bucket,s3_key}
                bucket = (
                    payload.get("s3_bucket")
                    or payload.get("bucket")
                    or payload.get("detail", {}).get("bucket")
                )
                key = (
                    payload.get("s3_key")
                    or payload.get("key")
                    or payload.get("detail", {}).get("key")
                )
                if bucket and key:
                    return bucket, key
                # Case 2: body itself is an S3 event
                s3_ev = _from_s3_event(payload)
                if s3_ev:
                    return s3_ev
    except Exception:
        logger.exception("Failed to parse SQS body for S3 details")
    return None


def _latest_json_under_prefix(bucket: str, prefix: str) -> Optional[str]:
    # List newest .json or .json.gz under optional prefix
    list_prefix = f"{prefix}/" if prefix else ""
    newest_key, newest_ts, token = None, None, None
    try:
        while True:
            kwargs = {"Bucket": bucket, "Prefix": list_prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                key_lower = obj["Key"].lower()
                if key_lower.endswith(".json") or key_lower.endswith(".json.gz"):
                    if newest_ts is None or obj["LastModified"] > newest_ts:
                        newest_ts, newest_key = obj["LastModified"], obj["Key"]
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
    except ClientError:
        logger.exception("Failed to list s3://%s/%s", bucket, list_prefix)
        return None
    if not newest_key:
        logger.warning("No JSON objects found under s3://%s/%s", bucket, list_prefix)
    return newest_key


def _decompress_if_needed(key: str, body_bytes: bytes, encoding: Optional[str]) -> str:
    # Detect gzip by key suffix, ContentEncoding, or magic bytes
    is_gzip = (
        (encoding or "").lower() == "gzip"
        or key.lower().endswith(".gz")
        or (len(body_bytes) >= 2 and body_bytes[0] == 0x1F and body_bytes[1] == 0x8B)
    )
    if is_gzip:
        try:
            return gzip.decompress(body_bytes).decode("utf-8", errors="replace")
        except Exception:
            # Some services double-wrap or stream gzip; fall back to fileobj path
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(body_bytes)) as gz:
                    return gz.read().decode("utf-8", errors="replace")
            except Exception:
                logger.exception("Gzip decompress failed; falling back to plain decode")
    return body_bytes.decode("utf-8", errors="replace")


def _get_json(bucket: str, key: str) -> Any:
    logger.info("Fetching s3://%s/%s", bucket, key)
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            parent = key.rsplit("/", 1)[0] if "/" in key else ""
            logger.warning("NoSuchKey for s3://%s/%s; listing siblings in s3://%s/%s/",
                           bucket, key, bucket, parent)
            try:
                resp = s3.list_objects_v2(Bucket=bucket, Prefix=(parent + "/" if parent else ""))
                nearby = [c["Key"] for c in resp.get("Contents", [])][:50]
                logger.warning("Nearby keys (first 50): %s", nearby)
            except ClientError:
                logger.exception("Also failed to list sibling keys")
        raise

    body_bytes: bytes = obj["Body"].read()
    body_text = _decompress_if_needed(key, body_bytes, obj.get("ContentEncoding"))
    try:
        return json.loads(body_text)
    except json.JSONDecodeError:
        logger.error("Object is not valid JSON (first 200 chars): %r", body_text[:200])
        raise


def _peek_items(doc: Any, limit: int) -> List[Any]:
    if isinstance(doc, list):
        return doc[:limit]
    if isinstance(doc, dict):
        for k in PREVIEW_KEYS:
            v = doc.get(k)
            if isinstance(v, list):
                return v[:limit]
        # Common pattern: {"data":{"items":[...]}}
        data = doc.get("data")
        if isinstance(data, dict):
            for k in PREVIEW_KEYS:
                v = data.get(k)
                if isinstance(v, list):
                    return v[:limit]
    # Nothing obvious to preview; avoid logging megabytes
    return []


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    bucket, processed_prefix = _env()
    limit = int(os.getenv("PRINT_ITEMS_LIMIT", "5"))
    logger.info("Env: bucket=%s processed_prefix=%s limit=%s", bucket, processed_prefix, limit)

    # Figure out which object to read
    resolved = _from_s3_event(event) or _from_sqs_event(event)
    if resolved:
        event_bucket, event_key = resolved
    else:
        # Fall back to the newest .json/.json.gz under the processed prefix
        event_bucket, event_key = bucket, _latest_json_under_prefix(bucket, processed_prefix)

    if not event_key:
        logger.warning("No key in event and no .json found under '%s/'", processed_prefix)
        return {"status": "no-input", "bucket": bucket, "prefix": processed_prefix}

    # Only prefix if needed (avoid processed/processed/…)
    if processed_prefix and not event_key.startswith(f"{processed_prefix}/"):
        full_key = f"{processed_prefix}/{event_key}"
    else:
        full_key = event_key

    target_bucket = event_bucket or bucket
    logger.info("Reading: s3://%s/%s", target_bucket, full_key)

    try:
        doc = _get_json(target_bucket, full_key)
    except Exception as e:
        logger.exception("Failed to read/parse s3://%s/%s", target_bucket, full_key)
        return {"status": "error", "bucket": target_bucket, "key": full_key, "error": str(e)}

    sample = _peek_items(doc, limit)

    # Helpful debug when preview is empty
    if not sample:
        top_keys = list(doc.keys())[:20] if isinstance(doc, dict) else "n/a"
        logger.info("Preview empty. Top-level keys: %s", top_keys)

    # Log a trimmed preview (avoid huge logs)
    try:
        logger.info("Sample (%d max): %s", limit, json.dumps(sample, ensure_ascii=False)[:4000])
    except Exception:
        logger.info("Sample (%d max) contains non-serializable items", limit)

    return {
        "status": "ok",
        "bucket": target_bucket,
        "key": full_key,
        "items_previewed": len(sample),
    }
