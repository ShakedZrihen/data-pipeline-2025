import json
import os

import pika

from ..database.handler import MongoDBClient
from ..enricher.patch import DataPatcher
from ..normalizer.normalize import DataNormalizer
from ..validator.validation import DataValidator


def ack_handler(func):
    def wrapper(ch, method, properties, body):
        try:
            func(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [ERROR]: {e}, sending to DLQ...")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    return wrapper


@ack_handler
def callback(ch, method, properties, body):
    data = json.loads(body)
    timestamp = data["timestamp"]

    print("Received message in RabbitMQ, validating and normalizing data...")
    # If the validator fails, the DLQ will recieve the raw message.
    data = DataNormalizer(data, timestamp=timestamp).normalize()
    DataValidator(data).validate_data()
    patcher = DataPatcher(data)
    data = patcher.enrich()

    print("saving file in database...")
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
        self.dlq = f"dead_{queue_name}"
        main_routing_key = f"to_{queue_name}"
        dlq_route_key = "to_dlq"

        self.main_exchange = f"{queue_name}.exchange"
        self.dead_exchange = f"{queue_name}.dead.exchange"

        queue_args = {
            "x-dead-letter-exchange": self.dead_exchange,
            "x-dead-letter-routing-key": dlq_route_key,
        }

        # Declare exchnages where the queue will run through
        self.channel.exchange_declare(
            exchange=self.main_exchange, exchange_type="direct", durable=True
        )
        self.channel.exchange_declare(
            exchange=self.dead_exchange, exchange_type="direct", durable=True
        )

        # Declare normal and dead queues
        self.channel.queue_declare(
            queue=self.queue_name, durable=True, arguments=queue_args
        )
        self.channel.queue_declare(queue=self.dlq, durable=True)

        # Bind normal and dead letter queue
        self.channel.queue_bind(
            exchange=self.main_exchange,
            queue=self.queue_name,
            routing_key=main_routing_key,
        )
        self.channel.queue_bind(
            exchange=self.dead_exchange, queue=self.dlq, routing_key=dlq_route_key
        )

    def receive_message(self):
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback, auto_ack=False
        )
        print(" [*] Waiting for messages...")
        self.channel.start_consuming()


if __name__ == "__main__":
    receiver = RabbitMQConsumer()
    receiver.receive_message()
