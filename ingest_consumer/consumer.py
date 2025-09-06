import os
import sys
import json
import time
import logging
import argparse
import boto3
import sqlite3
from dotenv import load_dotenv
from botocore.config import Config
from .storage_sqlite import save_message as sqlite_save
from .processor import process_raw_message, ProcessingError
from .errors import send_to_dlq
from ingest_consumer.enricher import enrich_message

load_dotenv()

# ---------- logging ----------
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.getLogger().handlers = [handler]
logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("ingest_consumer")
logger = logging.getLogger("ingest_consumer")

def _sqs():
    return boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        config=Config(retries={'max_attempts': 3, 'mode': 'standard'})
    )

def _receive_batch(queue_url: str, max_batch: int, visibility_timeout: int = 30, wait_time: int = 10):
    return _sqs().receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_batch,
        VisibilityTimeout=visibility_timeout,
        WaitTimeSeconds=wait_time,
        MessageAttributeNames=['All'],
        AttributeNames=['All'],
    )

def _delete_message(queue_url: str, receipt_handle: str):
    _sqs().delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

def run_loop(once: bool = False):
    queue_url = os.getenv("SQS_QUEUE_URL")
    if not queue_url:
        raise RuntimeError("Missing SQS_QUEUE_URL")

    max_batch = int(os.getenv("SQS_MAX_BATCH", "10"))
    visibility = int(os.getenv("VISIBILITY_TIMEOUT_SEC", "30"))

    log.info("Consumer started. queue=%s batch=%s", queue_url, max_batch)

    while True:
        resp = _receive_batch(queue_url, max_batch, visibility_timeout=visibility)
        messages = resp.get("Messages", [])
        if not messages:
            if once:
                log.info("No messages; exiting (once mode).")
                return
            time.sleep(1)
            continue

        log.info("Got %d messages", len(messages))

        for m in messages:
            body = m.get("Body", "")
            rh = m.get("ReceiptHandle")

            try:
                parsed = process_raw_message(body)
                logger.info(
                    "MSG provider=%s branch=%s type=%s ts=%s items_total=%s",
                    parsed["provider"], parsed["branch"], parsed["type"], parsed["timestamp"], parsed["items_total"]
                )
                sample = parsed.get("items_sample", [])[:3]
                if sample:
                    logger.info("sample: %s", sample)
                db_path = os.getenv("INGEST_SQLITE_PATH")
                if db_path:
                    try:
                        message_id = sqlite_save(db_path, parsed)
                        logger.info("Saved message %s to SQLite: %s", message_id, db_path)

                        if message_id is not None:
                            with sqlite3.connect(db_path) as conn:
                                enriched_rows = enrich_message(conn, message_id)
                            logger.info("Enriched message %s: %s rows", message_id, enriched_rows)
                        else:
                            logger.warning("Could not resolve message_id after save; skipping enrich.")

                    except Exception as e:
                        logger.warning("Failed to save/enrich SQLite: %s", e)
                preview_items = parsed.get("items_sample") or parsed.get("items") or []
                preview_items = preview_items[:3]
                log.info("MSG provider=%s branch=%s type=%s ts=%s items_total=%s",
                         parsed.get("provider"), parsed.get("branch"),
                         parsed.get("type"), parsed.get("timestamp"),
                         parsed.get("items_total", len(parsed.get("items", []))))
                log.info("sample: %s", json.dumps(preview_items, ensure_ascii=False))
                _delete_message(queue_url, rh)
            except ProcessingError as e:
                log.warning("ProcessingError: %s", e)
                send_to_dlq(body, str(e))
                _delete_message(queue_url, rh)
            except Exception as e:
                log.error("Unexpected error: %s", e, exc_info=True)

        if once:
            return

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Receive one poll and exit")
    args = parser.parse_args()
    run_loop(once=args.once)

if __name__ == "__main__":
    main()
