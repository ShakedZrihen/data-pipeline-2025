import os, json, boto3
from botocore.config import Config

region = os.getenv("AWS_REGION", "us-east-1")
endpoint = os.getenv("AWS_ENDPOINT_URL")

with open("sample_msg.json", "r", encoding="utf-8") as f:
    body = f.read()

sqs = boto3.client("sqs", region_name=region, endpoint_url=endpoint, config=Config())
queue_url = os.environ["SQS_QUEUE_URL"]

resp = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
print("Sent:", resp["MessageId"])
