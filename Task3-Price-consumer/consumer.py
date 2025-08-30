import os
import sys
import json
import logging
import argparse
from typing import Any, Dict, List

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

AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-central-1"
QUEUE_PROVIDER = os.getenv("QUEUE_PROVIDER", "sqs").lower()
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
DLQ_URL = os.getenv("DLQ_URL")
_sqs = boto3.client("sqs", region_name=AWS_REGION)

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

def _incr(metric: str, value: int = 1, **labels):
    """Tolerate metrics implementations without labels."""
    try:
        _metrics_incr(metric, value, **labels)
    except TypeError:
        _metrics_incr(metric, value)


def _json_loads_maybe_twice(s: str) -> Dict[str, Any]:
    """Tolerate UTF-8 BOM and double-encoded JSON (string of JSON)."""
    if s and s[0] == "\ufeff":
        s = s.lstrip("\ufeff")
    obj = json.loads(s)
    if isinstance(obj, str):
        if obj and obj[0] == "\ufeff":
            obj = obj.lstrip("\ufeff")
        obj = json.loads(obj)
    return obj


def _send_to_dlq(original_body: str, err_msg: str):
    if QUEUE_PROVIDER != "sqs":
        return
    if not DLQ_URL:
        logging.error("DLQ_URL not set; dropping invalid message. err=%s body=%s",
                      err_msg, (original_body or "")[:500])
        return
    try:
        _sqs.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps({"error": err_msg, "original": original_body}),
        )
        _incr("dlq.sent", 1)
    except (BotoCoreError, ClientError) as e:
        logging.exception("Failed sending to DLQ: %s", e)


def _db_exec(sql: str):
    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
        finally:
            cur.close()
    finally:
        conn.close()


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


def _process_doc(doc: Dict[str, Any]) -> int:
    norm = normalize_message(doc)
    validate_doc(norm)
    rows = doc_to_rows(norm)

    conn = get_conn()
    try:
        n = upsert_rows(conn, rows)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    _incr("db.upserted", n)
    return n

def lambda_handler(event, context):

    if isinstance(event, dict) and event.get("action") == "setup":
        _db_exec(DDL_PRICE_ITEMS)
        logging.info("setup: price_items ensured")
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
                body = rec.get("body", "")
                try:
                    doc = _json_loads_maybe_twice(body)
                    upserted = _process_doc(doc)
                    total += upserted
                    _incr("msg.ok", 1)
                except Exception as e:
                    _send_to_dlq(body, str(e))
                    failures.append({"itemIdentifier": rec.get("messageId", "unknown")})
                    logging.error("invalid message id=%s err=%s", rec.get("messageId"), e)

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
    msgs = _sqs_receive_batch(args.max_messages, args.wait, args.visibility)
    if not msgs:
        print(json.dumps({"ok": True, "received": 0}))
        return

    processed = 0
    with timer("ingest.batch.ms", provider="sqs", count=len(msgs)):
        for m in msgs:
            body = m.get("Body") or m.get("body") or ""
            try:
                doc = _json_loads_maybe_twice(body)
                upserted = _process_doc(doc)
                _sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=m["ReceiptHandle"])
                processed += 1
                logging.info("ok upserted=%s id=%s", upserted, m.get("MessageId"))
                _incr("msg.ok", 1)
            except Exception as e:
                _send_to_dlq(body, str(e))
                logging.error("invalid message id=%s err=%s", m.get("MessageId"), e)
                _incr("msg.invalid", 1)

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
