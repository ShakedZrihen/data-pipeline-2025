import os
import json
import logging
import boto3
from botocore.config import Config

log = logging.getLogger(__name__)

def _sqs():
    return boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        config=Config(retries={'max_attempts': 3, 'mode': 'standard'})
    )

def send_to_dlq(original_body: str, error_msg: str):
    dlq_url = os.getenv("SQS_DLQ_URL")
    if not dlq_url:
        log.warning("SQS_DLQ_URL not set; skipping DLQ send. error=%s", error_msg)
        return
    payload = {
        "error": error_msg,
        "original_body": original_body,
    }
    _sqs().send_message(
        QueueUrl=dlq_url,
        MessageBody=json.dumps(payload, ensure_ascii=False)
    )
    log.info("Sent message to DLQ")
