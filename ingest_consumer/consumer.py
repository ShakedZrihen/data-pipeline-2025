import os, json, time, logging
import boto3
from pydantic import ValidationError
from dotenv import load_dotenv
from validator import Envelope
from normalizer import enrich
from storage import connect, upsert_items
from dlq import send_to_dlq

load_dotenv()
log = logging.getLogger("consumer")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))

def sqs_client():
    return boto3.client("sqs", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))

def run_sqs():
    queue_url = os.getenv("SQS_QUEUE_URL")
    batch = int(os.getenv("BATCH_SIZE","10"))
    vis = int(os.getenv("VISIBILITY_TIMEOUT_SEC","30"))
    pg = connect(os.getenv("PG_DSN"))

    sqs = sqs_client()
    log.info("Polling SQS...")
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=batch, WaitTimeSeconds=10,
            VisibilityTimeout=vis
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            continue

        for m in msgs:
            body_text = m["Body"]
            try:
                data = json.loads(body_text)
                data = enrich(data)
                env = Envelope(**data).model_dump()
                upsert_items(pg, env)
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                log.info("Upsert OK | items=%d", len(env["items"]))
            except (json.JSONDecodeError, ValidationError) as e:
                send_to_dlq({"raw": body_text}, f"validation/decode error: {e}")
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                log.warning("Sent to DLQ: %s", e)
            except Exception as e:
                log.exception("Processing error (will be retried): %s", e)

if __name__ == "__main__":
    provider = os.getenv("QUEUE_PROVIDER","SQS").upper()
    if provider == "SQS":
        run_sqs()
    else:
        raise SystemExit("Only SQS wired in this demo; Rabbit/Kafka stubs TBD")
