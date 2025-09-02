import json
import logging
from sqs_consumer import SQSConsumer
from aggregator import Aggregator

log = logging.getLogger(__name__)
logging.basicConfig(level="INFO")

agg = Aggregator()  # OUT_DIR can be overridden via env

def handle(body: str) -> bool:
    # Always ack SQS messages (return True), even if invalid payload.
    try:
        payload = json.loads(body)
    except Exception:
        log.exception("Invalid JSON in SQS message; dropping.")
        return True

    # Merge into group; maybe flush immediately if all parts arrived
    out_path = agg.add(payload)
    if out_path:
        log.info("Wrote merged file: %s", out_path)

    # Opportunistic stale flush
    for p in agg.flush_stale():
        log.info("Flushed stale group: %s", p)

    return True  # ack/delete

if __name__ == "__main__":
    SQSConsumer().poll(handle)
