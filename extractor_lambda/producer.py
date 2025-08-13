import os
import json
import boto3
from botocore.config import Config

def _sqs_client():
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    region = os.getenv("AWS_REGION", "us-east-1")
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client("sqs", region_name=region, endpoint_url=endpoint, config=cfg)

def send_message(payload: dict):
    rabbit_url = os.getenv("RABBITMQ_URL")
    if rabbit_url:
        return _send_rabbit(payload, rabbit_url)
    queue_url = os.environ["OUTPUT_QUEUE_URL"]
    sqs = _sqs_client()
    body = json.dumps(payload, ensure_ascii=False)
    resp = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
    return {"transport": "sqs", "message_id": resp.get("MessageId")}

def _send_rabbit(payload: dict, url: str):
    import pika
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    exchange = os.getenv("RABBITMQ_EXCHANGE", "")
    routing_key = os.getenv("RABBITMQ_ROUTING_KEY", "extractor.json")
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
    connection.close()
    return {"transport": "rabbitmq", "routing_key": routing_key}
