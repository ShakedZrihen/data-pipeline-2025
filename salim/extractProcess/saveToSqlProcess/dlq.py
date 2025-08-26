import os
import json
import pika

DLQ_QUEUE = os.getenv("DLQ_QUEUE", "normalized.dlq")
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/%2f")

def publish_message(queue_name: str, message: str):
    params = pika.URLParameters(RABBIT_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=message.encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    connection.close()

def send_to_dlq(message: dict, reason: str, stage: str = "validation"):
    envelope = {
        "stage": stage,
        "reason": reason,
        "original": message,
    }
    publish_message(DLQ_QUEUE, json.dumps(envelope, ensure_ascii=False))
