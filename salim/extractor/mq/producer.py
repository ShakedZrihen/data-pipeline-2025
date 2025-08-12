import pika


class RabbitMQProducer:
    def __init__(self, host: str = "localhost", queue: str = "extractor_queue"):
        print(f"Connecting to RabbitMQ server at {host}")
        self.host = host
        self.queue = queue
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.host)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"Closing connection to RabbitMQ server at {self.host}")
        self.connection.close()

    def send_queue_message(self, msg: str):
        print(f"Sending message to RabbitMQ server at {self.host}")
        self.channel.basic_publish(exchange="", routing_key=self.queue, body=msg)
