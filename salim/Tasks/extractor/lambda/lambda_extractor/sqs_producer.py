# extractor/sqs_producer.py
import json
import boto3
from config import ENDPOINT_URL, AWS_REGION, SQS_QUEUE_NAME

def _sqs():
    return boto3.client("sqs", endpoint_url=ENDPOINT_URL, region_name=AWS_REGION)

def _queue_url():
    sqs = _sqs()
    sqs.create_queue(QueueName=SQS_QUEUE_NAME)  # idempotent in LocalStack
    return sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]

def send_json(message: dict):
    _sqs().send_message(QueueUrl=_queue_url(), MessageBody=json.dumps(message, ensure_ascii=False))
