import boto3
import os
from .config import SQS_QUEUE_URL, BATCH_SIZE, WAIT_TIME_SECONDS
from .logger import logger

SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL', 'http://localhost:4566/000000000000/my-queue')
BATCH_SIZE = int(os.getenv('SQS_BATCH_SIZE', 10))
WAIT_TIME_SECONDS = int(os.getenv('SQS_WAIT_TIME', 5))

sqs = boto3.client(
    'sqs',
    endpoint_url=os.getenv('SQS_ENDPOINT', 'http://localhost:4566'),
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

def receive_messages():
    response = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL,
        MaxNumberOfMessages=BATCH_SIZE,
        WaitTimeSeconds=WAIT_TIME_SECONDS
    )

    messages = response.get("Messages", [])
    logger.info(f"Received {len(messages)} messages")
    return messages

def delete_message(receipt_handle):
    sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
    logger.info("Deleted message from SQS")
