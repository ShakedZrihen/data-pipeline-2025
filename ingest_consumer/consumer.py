
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
from .processor import process_raw_message, ProcessingError
from .errors import send_to_dlq
from ingest_consumer.storage_sqlite import DDL
from ingest_consumer.db import init_db, get_db_path, save_message, upsert_supermarket
load_dotenv()

# ---------- logging ----------
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.getLogger().handlers = [handler]
logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest_consumer")
logger = logging.getLogger("ingest_consumer")
ENDPOINT = os.getenv("AWS_ENDPOINT_URL")
REGION = os.getenv("AWS_REGION", "us-east-1")
ACCESS = os.getenv("AWS_ACCESS_KEY_ID", "test")
SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
s3 = boto3.client("s3", region_name=REGION, endpoint_url=ENDPOINT,
                  aws_access_key_id=ACCESS, aws_secret_access_key=SECRET)
_S3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID","test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY","test"),
)
DB_PATH = get_db_path()
init_db(DB_PATH)
logging.info(f"DB READY at {DB_PATH}")
QUEUE_URL = os.environ.get("SQS_QUEUE_URL") or os.environ.get("OUTPUT_QUEUE_URL")
AWS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")
init_db(DB_PATH)
db_path = os.environ.get("DB_PATH", "data/prices.db")
logging.info(f"DB READY at {db_path}")
logging.info(f"BOOT: db_path={DB_PATH}")
logging.info(f"BOOT: queue={QUEUE_URL}")
logging.info(f"BOOT: aws_endpoint={AWS_ENDPOINT}")
def load_items_from_message(msg: dict):
    if "s3_bucket" in msg and "s3_key" in msg:
        obj = _S3.get_object(Bucket=msg["s3_bucket"], Key=msg["s3_key"])
        raw = obj["Body"].read().decode("utf-8")
        data = json.loads(raw)
        items = data.get("items") or data.get("items_sample") or []
        msg.setdefault("items_total", data.get("items_total", len(items)))
        return items
    return msg.get("items") or msg.get("items_sample") or []
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
    db_path = os.getenv("INGEST_SQLITE_PATH") or DB_PATH

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
            receipt = m.get("ReceiptHandle")

            try:
                parsed = process_raw_message(body)
                logger.info("MSG provider=%s branch=%s type=%s ts=%s items_total=%s",
                            parsed["provider"], parsed["branch"], parsed["type"],
                            parsed["timestamp"], parsed["items_total"])
                sample = (parsed.get("items_sample") or [])[:3]
                if sample:
                    logger.info("sample: %s", sample)
                message_id = save_message(DB_PATH, parsed)
                logger.info("Saved message %s to SQLite: %s", message_id, DB_PATH)
                upsert_supermarket(DB_PATH, str(parsed["provider"]), str(parsed["branch"]), str(parsed["provider"]))

                if not message_id:
                    logger.error("SQLite save returned None; dropping or DLQ to avoid reprocessing loop.")
                    try:
                        send_to_dlq(body, "sqlite_save returned no id")
                    except Exception as e:
                        logger.warning("DLQ not configured or failed: %s", e)
                else:
                    processed = dict(parsed)
                    processed["_sqs_message_id"] = message_id
                    processed.setdefault("ts", processed.get("timestamp"))

                    # try:
                    #     store_message_and_items(processed)
                    #     with sqlite3.connect(db_path) as conn:
                    #         enriched_rows = enrich_message(conn, message_id)
                    #     logger.info("Enriched message %s: %s rows", message_id, enriched_rows)
                    #     _delete_message(queue_url, receipt)
                    # except Exception as e:
                    #     logger.exception("FAILED to store/enrich message_id=%s: %s", message_id, e)
                    #     try:
                    #         send_to_dlq(body, f"store/enrich failed: {e}")
                    #     except Exception as e2:
                    #         logger.warning("DLQ not configured or failed: %s", e2)
            except ProcessingError as e:
                log.warning("ProcessingError: %s", e)
                send_to_dlq(body, str(e))
                _delete_message(queue_url, receipt)
            except Exception as e:
                log.error("Unexpected error: %s", e, exc_info=True)
                send_to_dlq(body, f"Unexpected error: {e}")
                _delete_message(queue_url, receipt)

        if once:
            return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Receive one poll and exit")
    args = parser.parse_args()
    run_loop(once=args.once)

if __name__ == "__main__":
    main()


