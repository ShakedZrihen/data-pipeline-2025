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
"""
from __future__ import annotations
import json
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

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
_SAVE_LOCAL_JSON = os.getenv("SAVE_LOCAL_JSON", "false").lower() == "true"


def _build_output_key(provider: str, branch: str, file_type: str, file_ts: str, chunk: Optional[int]) -> str:
    suffix = f"_b{chunk}" if chunk is not None else ""
    return f"processed/{provider}/{branch}/{file_type}_{file_ts}{suffix}.json"


def _process_streaming_and_upload(
    bucket: str,
    raw_key: str,
    provider: str,
    branch: str,
    file_type: str,
    file_ts: str,
    chunk: Optional[int],
) -> Dict[str, Any]:
    """
    Stream gzip from S3, stream XML items, normalize per-item, and stream JSON array to S3.
    Avoids loading the whole file into memory.
    """
    out_bucket = _OUTPUT_BUCKET or bucket
    out_key = _build_output_key(provider, branch, file_type, file_ts, chunk)

    # 1) Open streaming gzip directly from S3 (no in-memory decompress)
    gz_stream = _s3.open_gzip_stream(bucket, raw_key)

    # 2) Iterate raw items from XML stream
    raw_iter = _xml.iter_items(gz_stream, provider=provider, file_type=file_type)

    # 3) Stream-write normalized items to a local temp file as a JSON array, then upload to S3
    items_count = 0
    sample_items: List[Dict[str, Any]] = []

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as tmp:
        tmp_path = tmp.name
        tmp.write("[")
        first = True

        for raw in raw_iter:
            # Normalize per item
            norm_list = (
                _norm.normalize_promos([raw]) if file_type == "promoFull" else _norm.normalize_prices([raw])
            )
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
    _s3.upload_file(out_bucket, out_key, data, content_type="application/json")

    try:
        os.remove(tmp_path)
    except Exception:
        pass

    message: Dict[str, Any] = {
        "provider": provider,
        "branch": branch,
        "type": file_type,
        "timestamp": file_ts,
        "chunk": chunk,
        "items_count": items_count,
        "source_file": _s3.decoded_key(raw_key),
        "items_s3_key": out_key,
    }
    if _EMIT_ITEMS:
        message["items"] = sample_items
    if _SAVE_LOCAL_JSON:  # optional debug
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
