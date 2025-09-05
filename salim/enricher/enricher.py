import json
import logging
from sqs_consumer import SQSConsumer
from aggregator import Aggregator
from utils.file_sink import InboxSink
from utils.payload_enricher import enrich_payload 

log = logging.getLogger(__name__)
logging.basicConfig(level="INFO")

agg = Aggregator()
inbox = InboxSink()

def handle(body: str) -> bool:
    try:
        payload = json.loads(body)
    except Exception:
        log.exception("Invalid JSON in SQS message; dropping.")
        return True

    payload = enrich_payload(payload)                

    path = inbox.write(payload)
    log.info("Wrote inbox file: %s", path)

    try:
        out_path = agg.add_path(path)
        if out_path:
            log.info("Wrote merged file: %s", out_path)
    finally:
        inbox.remove(path)

    for p in agg.flush_stale():
        log.info("Flushed stale group: %s", p)

    return True

if __name__ == "__main__":
    SQSConsumer().poll(handle)
