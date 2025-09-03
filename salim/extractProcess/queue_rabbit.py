import json
from typing import Optional

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

    params = pika.URLParameters(RABBIT_URL)
    params.heartbeat = 0                      # ביטול heartbeats לפרויקט לימודי
    params.blocked_connection_timeout = 120   # אופציונלי

    _rabbit_conn = pika.BlockingConnection(params)
    _rabbit_channel = _rabbit_conn.channel()
    _rabbit_channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
    try:
        _rabbit_channel.confirm_delivery()
    except Exception:
        pass
    print(f"[Init] RabbitMQ queue declared: {RABBIT_QUEUE}")
    return _rabbit_channel

def _ensure_open():
    """פותח מחדש אם החיבור/ערוץ נסגרו (קטן ופשוט)."""
    global _rabbit_conn, _rabbit_channel
    if _rabbit_conn is None or _rabbit_conn.is_closed:
        ensure_rabbit()
    if _rabbit_channel is None or _rabbit_channel.is_closed:
        ensure_rabbit()

def emit_event_full_json(doc: dict):
    if QUEUE_BACKEND != "rabbit":
        print(f"[Queue] Skipped (QUEUE_BACKEND={QUEUE_BACKEND})")
        return
    if pika is None:
        raise RuntimeError("pika not installed — cannot use RabbitMQ")

    _ensure_open()
    payload = json.dumps(doc, ensure_ascii=False).encode("utf-8")

    def _publish_once():
        _rabbit_channel.basic_publish(
            exchange="",                 # default exchange → לפי שם התור
            routing_key=RABBIT_QUEUE,
            body=payload,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,        # persistent
            ),
            mandatory=False,
        )

    try:
        _publish_once()
    except Exception as e:
        # אם הערוץ נסגר/נפל החיבור—נפתח מחדש וננסה פעם אחת
        print(f"[Queue] publish failed once ({type(e).__name__}: {e}) — reopen & retry")
        ensure_rabbit()
        _publish_once()
