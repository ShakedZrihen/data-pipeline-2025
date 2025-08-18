import json
import os

import pika

from ..database.handler import MongoDBClient
from ..normalizer.normalize import DataNormalizer


def auto_ack(func):
    def wrapper(ch, method, properties, body):
        try:
            func(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [ERROR]: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    return wrapper


@auto_ack
def callback(ch, method, properties, body):
    data = json.loads(body)
    timestamp = data["timestamp"]
    data = DataNormalizer(data, timestamp=timestamp).normalize()
    print("Received message in RabbitMQ, saving in database")
    db = MongoDBClient("extracted_files")
    db.insert_document("files", data)
    print("inserted document.")


class RabbitMQConsumer:
    def __init__(self, queue_name: str = "extractor_queue"):
        host = os.environ["RABBIT_HOST"]
        port = int(os.getenv("RABBIT_PORT", "5672"))
        user = os.environ["RABBIT_USER"]
        pwd = os.environ["RABBIT_PASS"]
        vhost = os.getenv("RABBIT_VHOST", "/")
        creds = pika.PlainCredentials(user, pwd)

        params = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=vhost,
            credentials=creds,
            heartbeat=60,
            blocked_connection_timeout=10,
            connection_attempts=3,
            retry_delay=2,
        )
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name)

    def receive_message(self):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback, auto_ack=False
        )
        print(" [*] Waiting for messages...")
        self.channel.start_consuming()


if __name__ == "__main__":
    receiver = RabbitMQConsumer()
    receiver.receive_message()
