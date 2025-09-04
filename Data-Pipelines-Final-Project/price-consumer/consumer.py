import os 
import sys
import json
import logging
import argparse
from typing import Any, Dict, List
from datetime import datetime, timezone

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

from enrichment import enrich_row 
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from normalizer import normalize_message
from validator import validate_doc, doc_to_rows
from db import get_conn, upsert_rows, run_migration_file
from metrics import incr as _metrics_incr, timer

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOG = logging.getLogger("consumer")

AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-central-1"

COMMON_ENDPOINT = os.getenv("AWS_ENDPOINT_URL")
SQS_ENDPOINT = os.getenv("SQS_ENDPOINT_URL") or COMMON_ENDPOINT
S3_ENDPOINT  = os.getenv("S3_ENDPOINT_URL")  or COMMON_ENDPOINT
DDB_ENDPOINT = os.getenv("DDB_ENDPOINT_URL") or COMMON_ENDPOINT
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
DLQ_URL       = os.getenv("DLQ_URL")
QUEUE_PROVIDER = os.getenv("QUEUE_PROVIDER", "sqs")
_sqs = boto3.client("sqs", region_name=AWS_REGION, endpoint_url=SQS_ENDPOINT)
_s3  = boto3.client("s3",  region_name=AWS_REGION, endpoint_url=S3_ENDPOINT)
_ddb = boto3.client("dynamodb", region_name=AWS_REGION, endpoint_url=DDB_ENDPOINT) if DDB_ENDPOINT else None
DDB_RUNS_TABLE = os.getenv("DDB_RUNS_TABLE")  # optional

DDL_PRICE_ITEMS = """
CREATE TABLE IF NOT EXISTS public.price_items (
  provider   text        NOT NULL,
  branch     text        NOT NULL,
  doc_type   text        NOT NULL,
  ts         timestamptz NOT NULL,
  product    text        NOT NULL,
  unit       text        NOT NULL,
  price      numeric     NOT NULL,
  src_key    text        NULL,
  etag       text        NULL,
  updated_at timestamptz NOT NULL DEFAULT NOW(),
  CONSTRAINT price_items_pk PRIMARY KEY (provider, branch, doc_type, ts, product)
);
CREATE INDEX IF NOT EXISTS price_items_branch_type_ts_idx
  ON public.price_items (provider, branch, doc_type, ts DESC);
"""

DDL_ENRICH_ALTERS = """
ALTER TABLE public.price_items
  ADD COLUMN IF NOT EXISTS barcode text NULL,
  ADD COLUMN IF NOT EXISTS canonical_name text NULL,
  ADD COLUMN IF NOT EXISTS brand text NULL,
  ADD COLUMN IF NOT EXISTS category text NULL,
  ADD COLUMN IF NOT EXISTS size_value numeric NULL,
  ADD COLUMN IF NOT EXISTS size_unit text NULL,
  ADD COLUMN IF NOT EXISTS currency text NULL,
  ADD COLUMN IF NOT EXISTS promo_price numeric NULL,
  ADD COLUMN IF NOT EXISTS promo_text text NULL,
  ADD COLUMN IF NOT EXISTS in_stock boolean NULL;

CREATE INDEX IF NOT EXISTS price_items_barcode_idx ON public.price_items (barcode);
"""

def _ensure_db_schema():
    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(DDL_PRICE_ITEMS)
            cur.execute(DDL_ENRICH_ALTERS)
            conn.commit()
            LOG.info("DB schema ensured (base + enrichment columns).")
        finally:
            cur.close()
    finally:
        conn.close()


def _incr(metric: str, value: int = 1, **labels):
    try:
        _metrics_incr(metric, value, **labels)
    except TypeError:
        _metrics_incr(metric, value)

def _json_loads_maybe_twice(s: str) -> Any:
    if not s:
        return {}
    if s and s[0] == "\ufeff":
        s = s.lstrip("\ufeff")
    obj = json.loads(s)
    if isinstance(obj, str):
        if obj and obj[0] == "\ufeff":
            obj = obj.lstrip("\ufeff")
        obj = json.loads(obj)
    return obj


def _send_to_dlq(original_body: str, err_msg: str):
    if not DLQ_URL:
        LOG.error("DLQ_URL not set; dropping invalid message. err=%s body=%s",
                  err_msg, (original_body or "")[:500])
        return
    try:
        _sqs.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps({"error": err_msg, "original": original_body}),
        )
        _incr("dlq.sent", 1)
    except (BotoCoreError, ClientError) as e:
        LOG.exception("Failed sending to DLQ: %s", e)


def _fetch_s3_json(bucket: str, key: str) -> Dict[str, Any]:
    o = _s3.get_object(Bucket=bucket, Key=key)
    body = o["Body"].read()
    try:
        return json.loads(body.decode("utf-8-sig"))
    except Exception:
        return json.loads(body)


def _load_doc_from_msg(body: str) -> Dict[str, Any]:
    """
    Accepts:
      1) full doc: {"provider":.., "items":[...]}
      2) small S3 pointer: {"s3":{"bucket":"...","key":"..."}}
      3) AWS S3 Event: {"Records":[{"s3":{"bucket":{"name":"..."},"object":{"key":"..."}}}]}
    """
    obj = _json_loads_maybe_twice(body)

    if isinstance(obj, dict) and isinstance(obj.get("items"), list) and obj["items"]:
        return obj

    if isinstance(obj, dict) and isinstance(obj.get("s3"), dict):
        s3p = obj["s3"]
        bucket = s3p.get("bucket")
        key = s3p.get("key")
        if isinstance(bucket, dict):
            bucket = bucket.get("name")
        if key is None and isinstance(s3p.get("object"), dict):
            key = s3p["object"].get("key")

        if not (bucket and key):
            raise ValueError("s3 pointer missing bucket/key")
        LOG.info("Fetching doc via s3 pointer s3://%s/%s", bucket, key)
        return _fetch_s3_json(bucket, key)

    if isinstance(obj, dict) and isinstance(obj.get("Records"), list) and obj["Records"]:
        r0 = obj["Records"][0]
        s3r = r0.get("s3") or {}
        bucket_obj = s3r.get("bucket") or {}
        object_obj = s3r.get("object") or {}
        bucket = bucket_obj.get("name")
        key = object_obj.get("key")
        if not (bucket and key):
            raise ValueError("S3 event missing bucket.name/object.key")
        LOG.info("Fetching doc via S3 event s3://%s/%s", bucket, key)
        return _fetch_s3_json(bucket, key)

    raise ValueError("Unsupported message shape; need {'items':[...]}, or {'s3':{bucket,key}}, or S3 'Records' event")


def _db_ping():
    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            _ = cur.fetchone()
        finally:
            cur.close()
    finally:
        conn.close()

def _count_table() -> int:
    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM public.price_items")
            n = cur.fetchone()[0]
        finally:
            cur.close()
    finally:
        conn.close()
    return int(n)

def _apply_promos(conn, doc: Dict[str, Any]) -> int:
    if doc.get("type") != "promoFull":
        return 0
    provider = doc.get("provider")
    branch   = doc.get("branch")
    updated = 0
    cur = conn.cursor()
    try:
        for it in doc.get("items", []):
            try:
                promo_price = float(it.get("price", 0) or 0)
            except Exception:
                continue
            promo_text  = (it.get("promo_text") or it.get("product") or "").strip() or None
            barcode     = (it.get("barcode") or "").strip()
            product     = (it.get("product") or "").strip()

            if barcode:
                cur.execute(
                    """
                    UPDATE public.price_items
                    SET promo_price = %s, promo_text = %s, updated_at = NOW()
                    WHERE provider = %s AND branch = %s AND doc_type = 'pricesFull' AND barcode = %s
                    """,
                    (promo_price, promo_text, provider, branch, barcode),
                )
                updated += cur.rowcount
            else:
                cur.execute(
                    """
                    UPDATE public.price_items
                    SET promo_price = %s, promo_text = %s, updated_at = NOW()
                    WHERE provider = %s AND branch = %s AND doc_type = 'pricesFull' AND product = %s
                    """,
                    (promo_price, promo_text, provider, branch, product),
                )
                updated += cur.rowcount
    finally:
        cur.close()
    _incr("db.promo_updates", updated)
    return updated


def _process_doc(doc: Dict[str, Any]) -> int:
    norm = normalize_message(doc)
    validate_doc(norm)
    rows = doc_to_rows(norm)

    enriched = [enrich_row(r) for r in rows]

    conn = get_conn()
    try:
        n = upsert_rows(conn, enriched)
        if norm.get("type") == "promoFull":
            try:
                u = _apply_promos(conn, norm)
                LOG.info("propagated promos onto %s price rows", u)
            except Exception as e:
                LOG.warning("apply_promos failed (non-fatal): %s", e)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    _incr("db.upserted", n)
    return n

def _ddb_ensure_table():
    if not (_ddb and DDB_RUNS_TABLE):
        return
    try:
        _ddb.describe_table(TableName=DDB_RUNS_TABLE)
    except _ddb.exceptions.ResourceNotFoundException:
        LOG.info("Creating DynamoDB table %s", DDB_RUNS_TABLE)
        _ddb.create_table(
            TableName=DDB_RUNS_TABLE,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )

def _ddb_put_last_run(processed: int, received: int, ok_msgs: int, invalid_msgs: int):
    if not (_ddb and DDB_RUNS_TABLE):
        return
    ts = datetime.now(timezone.utc).isoformat()
    try:
        _ddb.put_item(
            TableName=DDB_RUNS_TABLE,
            Item={
                "pk": {"S": "price-consumer:last_run"},
                "ts": {"S": ts},
                "processed": {"N": str(processed)},
                "received": {"N": str(received)},
                "ok_msgs": {"N": str(ok_msgs)},
                "invalid_msgs": {"N": str(invalid_msgs)},
            },
        )
    except Exception as e:
        LOG.info("DDB put last_run failed: %s", e)


def lambda_handler(event, context):
    if isinstance(event, dict) and event.get("action") == "setup":
        _ensure_db_schema()
        return {"ok": True, "setup": "done"}

    if isinstance(event, dict) and event.get("action") == "probe":
        _db_ping()
        return {"ok": True}

    if isinstance(event, dict) and event.get("action") == "count":
        return {"count": _count_table()}

    if isinstance(event, dict) and "Records" in event:
        total = 0
        failures: List[Dict[str, str]] = []
        with timer("ingest.batch.ms", provider="sqs", count=len(event["Records"])):
            for rec in event["Records"]:
                body = rec.get("body") or rec.get("Body") or json.dumps(rec)
                try:
                    doc = _load_doc_from_msg(body)
                    upserted = _process_doc(doc)
                    total += upserted
                    _incr("msg.ok", 1)
                except Exception as e:
                    _send_to_dlq(body, str(e))
                    failures.append({"itemIdentifier": rec.get("messageId", "unknown")})
                    LOG.error("invalid message id=%s err=%s", rec.get("messageId"), e)

        if failures:
            return {"batchItemFailures": failures}
        return {"ok": True, "upserted": total}

    return {"error": "Unexpected event shape"}


def _cmd_migrate(args):
    sql_path = args.file or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "migrations", "001_create_price_items.sql")
    )
    conn = get_conn()
    try:
        run_migration_file(conn, sql_path)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    print(f"migration applied: {sql_path}")

def _cmd_consume_file(args):
    _ensure_db_schema()
    with open(args.path, "r", encoding="utf-8-sig") as f:
        doc = json.load(f)
    with timer("ingest.file.ms"):
        n = _process_doc(doc)
    print(f"Upserted {n} records from {args.path}")

def _sqs_receive_batch(max_messages: int, wait_seconds: int, visibility: int):
    if QUEUE_PROVIDER != "sqs":
        print("Only SQS is implemented. Set QUEUE_PROVIDER=sqs.", file=sys.stderr)
        sys.exit(2)
    if not SQS_QUEUE_URL:
        print("SQS_QUEUE_URL is not set", file=sys.stderr)
        sys.exit(2)
    return _sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=max(1, min(10, max_messages)),
        WaitTimeSeconds=wait_seconds,
        VisibilityTimeout=visibility,
    ).get("Messages", [])

def _cmd_consume_batch(args):
    _ensure_db_schema()
    try:
        _ddb_ensure_table()
    except Exception as e:
        LOG.info("Skipping DDB ensure (non-fatal): %s", e)

    msgs = _sqs_receive_batch(args.max_messages, args.wait, args.visibility)
    if not msgs:
        print(json.dumps({"ok": True, "received": 0}))
        return

    processed = 0
    ok_msgs = 0
    invalid_msgs = 0
    with timer("ingest.batch.ms", provider="sqs", count=len(msgs)):
        for m in msgs:
            body = m.get("Body") or m.get("body") or ""
            try:
                doc = _load_doc_from_msg(body)
                upserted = _process_doc(doc)
                _sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=m["ReceiptHandle"])
                processed += upserted
                ok_msgs += 1
                LOG.info("ok upserted=%s id=%s", upserted, m.get("MessageId") or m.get("messageId"))
                _incr("msg.ok", 1)
            except Exception as e:
                invalid_msgs += 1
                _send_to_dlq(body, str(e))
                LOG.error("invalid message id=%s err=%s", m.get("MessageId") or m.get("messageId"), e)
                _incr("msg.invalid", 1)

    try:
        _ddb_put_last_run(processed=processed, received=len(msgs), ok_msgs=ok_msgs, invalid_msgs=invalid_msgs)
    except Exception as e:
        LOG.info("Skipping DDB last_run put (non-fatal): %s", e)

    print(json.dumps({"ok": True, "processed": processed, "received": len(msgs)}))

def main():
    p = argparse.ArgumentParser(description="Queue consumer: normalize/enrich/validate → PostgreSQL (idempotent)")
    sub = p.add_subparsers(dest="cmd")
    sub.required = True

    sp = sub.add_parser("migrate")
    sp.add_argument("--file", help="Path to SQL migration file (defaults to migrations/001_create_price_items.sql)")
    sp.set_defaults(func=_cmd_migrate)

    sp = sub.add_parser("consume-file")
    sp.add_argument("path", help="Path to JSON document")
    sp.set_defaults(func=_cmd_consume_file)

    sp = sub.add_parser("consume-batch")
    sp.add_argument("--max-messages", type=int, default=int(os.getenv("SQS_BATCH_SIZE", "10")))
    sp.add_argument("--wait", type=int, default=int(os.getenv("SQS_WAIT_TIME_SECONDS", "10")))
    sp.add_argument("--visibility", type=int, default=int(os.getenv("SQS_VISIBILITY_TIMEOUT", "30")))
    sp.set_defaults(func=_cmd_consume_batch)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
