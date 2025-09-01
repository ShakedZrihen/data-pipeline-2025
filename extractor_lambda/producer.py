import os, json, logging

try:
    import boto3
except Exception:
    boto3 = None

def send_message(payload: dict):
    queue_url = os.environ.get("OUTPUT_QUEUE_URL")
    if not queue_url:
        logging.info("[producer] OUTPUT_QUEUE_URL not set -> local mode (skip SQS send).")
        return

    if boto3 is None:
        logging.warning("[producer] boto3 not available; cannot send to SQS.")
        return

    region = os.environ.get("AWS_REGION", "eu-west-1")
    sqs = boto3.client("sqs", region_name=region)
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload, ensure_ascii=False))
    logging.info("[producer] sent to SQS: %s", queue_url)


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
