from .config import DLQ_URL
from .logger import logger
import os
import json
import boto3

os.environ.setdefault('S3_ENDPOINT', 'http://localhost:4566')
os.environ.setdefault('SQS_ENDPOINT', 'http://localhost:4566')
os.environ.setdefault('SQS_QUEUE_URL', 'http://localhost:4566/000000000000/my-queue')
os.environ.setdefault('S3_BUCKET', 'test-bucket')

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv('S3_ENDPOINT'),
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

def send_to_dlq(original_message, error_msg):
    wrapped = {
        "original": original_message,
        "error": error_msg
    }
    sqs.send_message(QueueUrl=DLQ_URL, MessageBody=json.dumps(wrapped))
    logger.warning("Message sent to DLQ")
