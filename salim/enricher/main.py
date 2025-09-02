# enricher/main.py
from sqs_consumer import SQSConsumer

def handle(body: str) -> bool:
    print("[MSG]", (body[:300] + "...") if len(body) > 300 else body)
    return True  # ack/delete

if __name__ == "__main__":
    SQSConsumer().poll(handle)
