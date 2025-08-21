import json
from typing import Optional

# i found out that its better to wrap some imports with "try" so there wont be harbu darbu if it wont no succseed, so the code can still run
try:
    import pika
except Exception:
    pika = None

from .config import QUEUE_BACKEND, RABBIT_URL, RABBIT_QUEUE

_rabbit_conn = None
_rabbit_channel = None

def ensure_rabbit():
    global _rabbit_conn, _rabbit_channel
    if QUEUE_BACKEND != "rabbit":
        return None
    if pika is None:
        raise RuntimeError("pika not installed — cannot use RabbitMQ")
    _rabbit_conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    _rabbit_channel = _rabbit_conn.channel()
    _rabbit_channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
    try:
        _rabbit_channel.confirm_delivery()
    except Exception:
        pass
    print(f"[Init] RabbitMQ queue declared: {RABBIT_QUEUE}")
    return _rabbit_channel

def emit_event_full_json(doc: dict):
    if QUEUE_BACKEND != "rabbit":
        print(f"[Queue] Skipped (QUEUE_BACKEND={QUEUE_BACKEND})")
        return
    if not _rabbit_channel:
        raise RuntimeError("Rabbit channel not initialized")
    payload = json.dumps(doc, ensure_ascii=False).encode("utf-8")
    _rabbit_channel.basic_publish(
        exchange="",
        routing_key=RABBIT_QUEUE,
        body=payload,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        ),
        mandatory=False,
    )
