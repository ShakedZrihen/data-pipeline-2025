import pika
from shared.config import settings

def channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.rabbitmq_host))
    ch = connection.channel()
    ch.queue_declare(queue=settings.rabbitmq_queue, durable=True)
    return connection, ch

def publish_json(payload: dict):
    import json
    connection, ch = channel()
    ch.basic_publish(
        exchange="",
        routing_key=settings.rabbitmq_queue,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2), # persistent
    )
    connection.close()
