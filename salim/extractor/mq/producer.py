import os

import pika


class RabbitMQProducer:
    def __init__(self, host: str = "localhost", queue: str = "extractor_queue"):
        print(f"Connecting to RabbitMQ server at {host}")

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

        self.host = host
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        self.main_exchange = f"{queue}.exchange"
        self.routing_key = f"to_{queue}"

        self.channel.exchange_declare(
            exchange=self.main_exchange, exchange_type="direct", durable=True
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"Closing connection to RabbitMQ server at {self.host}")
        self.connection.close()

    def send_queue_message(self, msg):
        print(f"Sending message to RabbitMQ server at {self.host}")
        self.channel.basic_publish(
            exchange=self.main_exchange,
            routing_key=self.routing_key,
            body=msg,
            mandatory=False,
        )
