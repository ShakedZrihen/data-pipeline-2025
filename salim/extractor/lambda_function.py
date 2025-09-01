# -*- coding: utf-8 -*-
"""
S3 -> Extract -> Normalize -> (SQS or RabbitMQ) + LastRun DB  [STREAMING VERSION]

Triggers on *.gz keys like:
  providers/<provider>/<branch>/(pricesFull|promoFull)_<timestamp>[_bN[_...]].gz

ENV:
  SQS_QUEUE_URL              (required if TRANSPORT=sqs)
  TRANSPORT                  ("sqs" | "rabbitmq", default: "sqs")

  OUTPUT_BUCKET              (optional; if absent, reuse input bucket)
  EMIT_ITEMS_IN_SQS          ("true"|"false", default: "false")  # if true, include up to SQS_ITEMS_LIMIT items
  SQS_ITEMS_LIMIT            (int, default: 100)

  SAVE_LOCAL_JSON            ("true"|"false", default: "false")  # writes /tmp/debug.json (non-streaming fallback)

  DB_TYPE                    ("local" | "dynamo" | "mongo", default: "local")
  DB_TABLE                   (DynamoDB table, default: "salim_last_runs")
  DYNAMO_REGION              (default: "il-central-1")
  MONGO_URI                  (if DB_TYPE="mongo")
  MONGO_DB                   (default: "salim")
  LOCAL_DB_FILE              (default: "/tmp/last_runs.json")


Notes:
- This version normalizes the chunk suffix to avoid keys like "_b_b2".
- SQS message includes both items_s3_bucket and items_s3_key for the consumer.
"""
from __future__ import annotations
import json
import logging
import os
import tempfile
import hashlib
from typing import Any, Dict, List, Optional, Union

import boto3

# Flexible imports (flat vs src/ layout)
try:
    from src.s3_handler import S3Handler
    from src.xml_processor import XMLProcessor
    from src.normalizer import DataNormalizer
    from src.sqs_producer import MessageProducer
    from src.db_handler import DatabaseHandler
except ImportError:  # Lambda flat layout
    from s3_handler import S3Handler
    from xml_processor import XMLProcessor
    from normalizer import DataNormalizer
    from sqs_producer import MessageProducer
    from db_handler import DatabaseHandler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Reused singletons across invokes
_s3_client = boto3.client("s3")
_s3 = S3Handler(_s3_client)
_xml = XMLProcessor()
_norm = DataNormalizer()
_db = DatabaseHandler(
    db_type=os.getenv("DB_TYPE", "local"),
    table_name=os.getenv("DB_TABLE", "salim_last_runs"),
    region=os.getenv("DYNAMO_REGION", "il-central-1"),
    connection_string=os.getenv("MONGO_URI", ""),
    database=os.getenv("MONGO_DB", "salim"),
    local_file=os.getenv("LOCAL_DB_FILE", "/tmp/last_runs.json"),
)
_producer = MessageProducer(
    transport=os.getenv("TRANSPORT", "sqs"),
    sqs_queue_url=os.getenv("SQS_QUEUE_URL"),
    rabbitmq_url=os.getenv("RABBITMQ_URL"),
    rabbitmq_exchange=os.getenv("RABBITMQ_EXCHANGE", ""),
    rabbitmq_routing_key=os.getenv("RABBITMQ_ROUTING_KEY", "prices.events"),
)

_OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET")
_EMIT_ITEMS = os.getenv("EMIT_ITEMS_IN_SQS", "false").lower() == "true"
_ITEMS_LIMIT = int(os.getenv("SQS_ITEMS_LIMIT", "100"))
_SQS_CHUNK_MODE = os.getenv("SQS_CHUNK_MODE", "true").lower() == "true"
_SQS_CHUNK_MAX_BYTES = int(os.getenv("SQS_CHUNK_MAX_BYTES", "230000"))
_SQS_CHUNK_MAX_ITEMS = int(os.getenv("SQS_CHUNK_MAX_ITEMS", "500"))
_SAVE_LOCAL_JSON = os.getenv("SAVE_LOCAL_JSON", "false").lower() == "true"


def _chunk_suffix(chunk: Optional[Union[int, str]]) -> str:
    """
    Normalize 'chunk' into a suffix like '', '_b2', '_b10'.
    Accepts int (2) or string ('_b2', 'b2', '2').
    Ensures we don't end up with duplicate '_b' prefixes such as '_b_b2'.
    """
    if chunk is None:
        return ""
    s = str(chunk).strip()
    if not s:
        return ""
    if s.startswith("_b"):
        return s                         # already normalized
    if s.startswith("b") and s[1:].isdigit():
        return f"_{s}"                   # 'b2' -> '_b2'
    if s.isdigit():
        return f"_b{s}"                  # '2' -> '_b2'
    # Fallback: preserve but ensure it starts with underscore
    return s if s.startswith("_") else f"_{s}"


def _iter_normalized_items_from_gzip(provider: str, file_type: str, bucket: str, raw_key: str):
    """
    Generator: stream gzip from S3, stream XML items, normalize per item, and yield normalized items.
    """
    with _s3.open_gzip_stream(bucket, raw_key) as gz:
        for raw in _xml.iter_items(gz, provider=provider, file_type=file_type):
            if file_type.startswith("price"):
                norm_list = _norm.normalize_prices([raw])
            else:
                norm_list = _norm.normalize_promos([raw])
            for n in norm_list:
                if n:
                    yield n


def _iter_item_chunks(envelope: dict, items_iter, max_bytes: int, max_items: int):
    """
    Yield item-lists where JSON({**envelope, "items": items}) stays under max_bytes and max_items.
    Uses exact JSON encoding per append to be safe under SQS 256KB limit.
    """
    chunk: list = []
    for item in items_iter:
        attempt = chunk + [item]
        body = json.dumps({**envelope, "items": attempt}, ensure_ascii=False).encode("utf-8")
        if len(attempt) <= max_items and len(body) <= max_bytes:
            chunk.append(item)
        else:
            if not chunk:
                # Extremely large single item (shouldn't happen) – send it alone
                yield [item]
                chunk = []
            else:
                yield chunk
                chunk = [item]
    if chunk:
        yield chunk

def _build_output_key(
    provider: str,
    branch: str,
    file_type: str,
    file_ts: str,
    chunk: Optional[Union[int, str]],
) -> str:
    suffix = _chunk_suffix(chunk)
    return f"processed/{provider}/{branch}/{file_type}_{file_ts}{suffix}.json"



def _process_streaming_and_upload(
    bucket: str,
    raw_key: str,
    provider: str,
    branch: str,
    file_type: str,
    file_ts: str,
    chunk: Optional[Union[int, str]],
) -> Dict[str, Any]:
    """
    Stream gzip from S3. Two modes:
      - Chunk-to-SQS (when _SQS_CHUNK_MODE=True): stream -> normalize -> send chunks (no S3 pointers)
      - Fallback S3 upload (when _SQS_CHUNK_MODE=False): write JSON array to S3 and send pointer
    """
    out_bucket = _OUTPUT_BUCKET or bucket

    # --- Chunked-to-SQS mode (no pointers) ---
    if _SQS_CHUNK_MODE:
        gid_src = f"{provider}|{branch}|{file_type}|{file_ts}|{_s3.decoded_key(raw_key)}"
        group_id = hashlib.sha1(gid_src.encode("utf-8")).hexdigest()

        envelope = {
            "schema": "extractor.items.v1",
            "provider": provider,
            "branch": branch,
            "type": file_type,
            "timestamp": file_ts,
            "group_id": group_id,
            "source_file": _s3.decoded_key(raw_key),
            "kind": "chunk",
        }

        items_iter = _iter_normalized_items_from_gzip(provider, file_type, bucket, raw_key)

        total_items = 0
        chunk_seq = 0

        for items in _iter_item_chunks(envelope, items_iter, _SQS_CHUNK_MAX_BYTES, _SQS_CHUNK_MAX_ITEMS):
            if not items:
                continue
            chunk_seq += 1
            total_items += len(items)
            env = dict(envelope)
            env["chunk_seq"] = chunk_seq
            env["chunk_total"] = None  # unknown until finished
            _producer.send_chunk(env, items)

        # Final manifest
        manifest = dict(envelope)
        manifest["kind"] = "manifest"
        manifest["chunk_total"] = chunk_seq
        manifest["items_count"] = total_items
        _producer.send(manifest)

        return {
            "schema": "extractor.items.v1",
            "provider": provider,
            "branch": branch,
            "type": file_type,
            "timestamp": file_ts,
            "chunk": _chunk_suffix(chunk),
            "items_count": total_items,
            "transport": _producer.transport,
            "group_id": group_id,
            "s3_mode": False,
        }

    # --- Legacy S3 upload mode (pointer message) ---
    out_key = _build_output_key(provider, branch, file_type, file_ts, chunk)

    # stream -> write JSON array to /tmp -> upload to S3
    tmp_path = "/tmp/items.json"
    items_count = 0
    sample_items: List[Dict[str, Any]] = []

    with _s3.open_gzip_stream(bucket, raw_key) as gz, open(tmp_path, "w", encoding="utf-8") as tmp:
        tmp.write("[")
        first = True
        for raw in _xml.iter_items(gz, provider=provider, file_type=file_type):
            if file_type.startswith("price"):
                norm_list = _norm.normalize_prices([raw])
            else:
                norm_list = _norm.normalize_promos([raw])
            if not norm_list:
                continue
            norm = norm_list[0]
            if not first:
                tmp.write(",")
            tmp.write(json.dumps(norm, ensure_ascii=False))
            first = False
            items_count += 1
            if _EMIT_ITEMS and len(sample_items) < _ITEMS_LIMIT:
                sample_items.append(norm)
        tmp.write("]")

    with open(tmp_path, "rb") as f:
        data = f.read()
    logger.info("Uploading s3://%s/%s (%d bytes)", out_bucket, out_key, len(data))
    _s3.upload_file(out_bucket, out_key, data, content_type="application/json")
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    message: Dict[str, Any] = {
        "schema": "extractor.items.v1",
        "provider": provider,
        "branch": branch,
        "type": file_type,
        "timestamp": file_ts,
        "chunk": _chunk_suffix(chunk),
        "items_count": items_count,
        "source_file": _s3.decoded_key(raw_key),
        "items_s3_bucket": out_bucket,
        "items_s3_key": out_key,
        "s3_mode": True,
    }
    if _EMIT_ITEMS:
        message["items"] = sample_items
    if _SAVE_LOCAL_JSON:
        try:
            with open("/tmp/debug.json", "w", encoding="utf-8") as f:
                json.dump(message, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    return message
def process_s3_record(record: Dict[str, Any]) -> Dict[str, Any]:
    bucket = record["s3"]["bucket"]["name"]
    raw_key = record["s3"]["object"]["key"]  # may be URL-encoded (Hebrew/spaces/+)
    size = int(record["s3"]["object"].get("size", 0))

    provider, branch, file_type, file_ts, chunk = _s3.parse_s3_key(raw_key)
    logger.info(
        "Parsed key -> provider=%s branch=%s type=%s ts=%s chunk=%s",
        provider, branch, file_type, file_ts, chunk
    )

    if size == 0:
        logger.warning("Skipping zero-byte object s3://%s/%s", bucket, raw_key)
        return {
            "provider": provider,
            "branch": branch,
            "type": file_type,
            "timestamp": file_ts,
            "items_count": 0,
            "skipped": "empty_object",
        }

    message = _process_streaming_and_upload(
        bucket=bucket,
        raw_key=raw_key,
        provider=provider,
        branch=branch,
        file_type=file_type,
        file_ts=file_ts,
        chunk=chunk,
    )

    logger.info(
        "Sending to %s (%d bytes) -> %s",
        _producer.transport, len(json.dumps(message, ensure_ascii=False)),
        _producer.sqs_queue_url or _producer.rabbitmq_url or "n/a"
    )
    _producer.send(message)

    if file_ts and file_ts != "unknown":
        _db.update_last_run(provider, branch, file_type, file_ts)

    return {
        "provider": provider,
        "branch": branch,
        "type": file_type,
        "timestamp": file_ts,
        "items_count": message["items_count"],
        "transport": _producer.transport,
    }


def handler(event, context):
    logger.info("Incoming event: %s", json.dumps(event))
    results: List[Dict[str, Any]] = []
    for rec in event.get("Records", []):
        try:
            results.append(process_s3_record(rec))
        except Exception as e:
            logger.exception("Failed to process record: %s", e)
    return {"processed": len(results), "results": results}


lambda_handler = handler
