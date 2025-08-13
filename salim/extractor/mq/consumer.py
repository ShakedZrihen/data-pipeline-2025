import json
import pika

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
    data = DataNormalizer(data,timestamp=timestamp).normalize()
    print(f"Received message on channel: {ch}, saving to example.json")
    with open("res.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


class RabbitMQConsumer:
    def __init__(self, queue_name: str = "extractor_queue", host: str = "localhost"):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
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
