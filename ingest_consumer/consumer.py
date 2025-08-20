from __future__ import annotations
import os, json, time, logging
import boto3
from botocore.config import Config
from prometheus_client import Counter, start_http_server
from .validator import Envelope
from .normalizer import normalize
from .storage import connect, upsert_items
from .dlq import send_to_dlq

log = logging.getLogger("consumer")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# --- Metrics ---
MESSAGES_RECEIVED = Counter("messages_received_total", "Messages pulled from SQS")
MESSAGES_PROCESSED = Counter("messages_processed_total", "Messages processed OK")
MESSAGES_FAILED = Counter("messages_failed_total", "Messages failed")
DB_UPSERTS = Counter("db_upserts_total", "Rows upserted to DB")

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

def main():
    port = int(os.getenv("METRICS_PORT", "8000"))
    start_http_server(port)
    log.info("Metrics at http://localhost:%d/metrics", port)
    run_sqs()

if __name__ == "__main__":
    main()
