import sys
import os
import json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from enricher import enrich_items_from_extractor_json
from utils.queue_handler import QueueHandler

def lambda_handler(event, context):
    queue_handler = QueueHandler()

    """Start consuming messages from the queue (blocking operation)"""
    if not queue_handler.connection or not queue_handler.channel:
        if not queue_handler.setup_rabbitmq():
            print("[error] Failed to setup RabbitMQ connection")
            return

    def _callback(chx, method, props, body):
        try:
            msg = json.loads(body)
        except Exception as e:
            print(f"[error] bad JSON: {e}")
            chx.basic_ack(delivery_tag=method.delivery_tag)
            return
        try:
            queue_handler.handle_message(msg, enrich_items_from_extractor_json)
        finally:
            chx.basic_ack(delivery_tag=method.delivery_tag)

    print(f"[consumer] waiting on '{queue_handler.queue_name}' at {queue_handler.rabbitmq_host}:{queue_handler.rabbitmq_port}")
    queue_handler.channel.basic_qos(prefetch_count=1)
    queue_handler.channel.basic_consume(queue=queue_handler.queue_name, on_message_callback=_callback)
    queue_handler.channel.start_consuming()

def main():
    lambda_handler(None, None)

if __name__ == "__main__":
    main()