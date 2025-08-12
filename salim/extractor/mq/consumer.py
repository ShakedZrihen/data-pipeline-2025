import pika


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
    print(f"Received message: {body}")


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
