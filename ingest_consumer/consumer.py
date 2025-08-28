from __future__ import annotations
import os, json, time, logging
import boto3
from botocore.config import Config
from prometheus_client import Counter, start_http_server, Histogram
from .validator import Envelope
from .normalizer import normalize
from .storage import connect, upsert_items
from .dlq import send_to_dlq
from .enrich import enrich_item
from .config import settings
from time import perf_counter

log = logging.getLogger("consumer")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# --- Metrics ---
MESSAGES_RECEIVED = Counter("messages_received_total", "Messages pulled from SQS")
MESSAGES_PROCESSED = Counter("messages_processed_total", "Messages processed OK")
MESSAGES_FAILED = Counter("messages_failed_total", "Messages failed")
DB_UPSERTS = Counter("db_upserts_total", "Rows upserted to DB")
MSG_TOTAL = Counter("messages_total", "messages processed", ["outcome"])
PROC_LAT  = Histogram("message_process_seconds", "message processing time (s)")
def _sqs():
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client("sqs",
                        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
                        region_name=os.getenv("AWS_REGION","us-east-1"),
                        config=cfg)

def run_sqs():
    queue_url = os.getenv("SQS_QUEUE_URL")
    if not queue_url:
        raise RuntimeError("SQS_QUEUE_URL missing")

    batch_size = int(os.getenv("BATCH_SIZE", "10"))
    wait_time = int(os.getenv("WAIT_TIME_SECONDS", "10"))
    visibility = int(os.getenv("VISIBILITY_TIMEOUT", "30"))

    sqs = _sqs()
    pg = connect(os.getenv("PG_DSN"))

    log.info("Polling SQS...")
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=batch_size,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility
        )

        messages = resp.get("Messages", [])
        if not messages:
            continue

        MESSAGES_RECEIVED.inc(len(messages))

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            body = msg["Body"]
            try:
                env = Envelope.model_validate_json(body)
                env = normalize(env)
                payload = env.model_dump()
                rows = upsert_items(pg, payload)
                DB_UPSERTS.inc(rows)
                MESSAGES_PROCESSED.inc()
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            except Exception as e:
                MESSAGES_FAILED.inc()
                log.warning("Sent to DLQ: %s", e)
                send_to_dlq(body, e, queue_name="extractor-results")
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)

# -------- logs --------
logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO),
                    format='%(message)s')
log = logging.getLogger("consumer")

def logj(**kw): log.info(json.dumps(kw, ensure_ascii=False))

def sqs():
    return boto3.client(
        "sqs",
        region_name=settings.aws_region,
        endpoint_url=settings.aws_endpoint_url
    )

def send_to_dlq(message_body: str, error: str):
    if not settings.sqs_dlq_url: return
    body = {"error": error, "message": {"raw": message_body}}
    sqs().send_message(QueueUrl=settings.sqs_dlq_url, MessageBody=json.dumps(body, ensure_ascii=False))

@PROC_LAT.time()
def process_batch(pg, msgs):
    ok_receipts = []
    rows = []
    for m in msgs:
        body_str = m["Body"]
        try:
            data = json.loads(body_str)
        except Exception as e:
            MSG_TOTAL.labels(outcome="json_invalid").inc()
            send_to_dlq(body_str, f"validation/decode error: {e}")
            logj(event="message_failed", reason="json_invalid", sample=body_str[:120])
            continue

        try:
            env = Envelope.model_validate(data)
        except Exception as e:
            MSG_TOTAL.labels(outcome="schema_invalid").inc()
            send_to_dlq(body_str, f"validation/schema error: {e}")
            logj(event="message_failed", reason="schema_invalid")
            continue

        # enrich
        for it in env.items:
            rows.append(enrich_item(env.provider, env.branch, env.type, env.timestamp, it.model_dump()))

        ok_receipts.append({"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]})

    if rows:
        n = upsert_items(pg, rows)
        MSG_TOTAL.labels(outcome="ok").inc(len(rows))
        logj(event="db_upsert", rows=n)

    if ok_receipts:
        sqs().delete_message_batch(QueueUrl=settings.sqs_queue_url, Entries=ok_receipts)
        logj(event="deleted_from_sqs", count=len(ok_receipts))

def run():
    start_http_server(settings.metrics_port)
    logj(event="metrics_listen", url=f"http://localhost:{settings.metrics_port}/metrics")
    pg = connect(settings.pg_dsn)
    client = sqs()
    while True:
        t0 = perf_counter()
        resp = client.receive_message(
            QueueUrl=settings.sqs_queue_url,
            MaxNumberOfMessages=settings.sqs_max_batch,
            WaitTimeSeconds=10,
            VisibilityTimeout=30
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            time.sleep(0.5); continue
        process_batch(pg, msgs)
        logj(event="batch_done", took_ms=int((perf_counter()-t0)*1000), size=len(msgs))

if __name__ == "__main__":
    run()
def main():
    port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(port)
    log.info("Metrics at http://localhost:%d/metrics", port)
    run_sqs()


