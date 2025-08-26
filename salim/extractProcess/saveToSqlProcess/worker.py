import os, json, pika, sys, traceback

# ייבוא השכבות שבנית
from extractProcess.saveToSqlProcess.processor import process_item
from extractProcess.saveToSqlProcess.dlq import send_to_dlq

RABBIT_URL   = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/%2f")
INPUT_QUEUE  = os.getenv("INPUT_QUEUE", "results-queue")
DLQ_QUEUE    = os.getenv("DLQ_QUEUE", "results.dlq")
PREFETCH     = int(os.getenv("PREFETCH", "50"))

def _connect():
    params = pika.URLParameters(RABBIT_URL)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.basic_qos(prefetch_count=PREFETCH)
    ch.queue_declare(queue=INPUT_QUEUE, durable=True)
    ch.queue_declare(queue=DLQ_QUEUE, durable=True)
    return conn, ch

def _split_items(doc):
    items = doc.get("items") or []
    t = (doc.get("type") or "").strip()
    for it in items:
        pid = it.get("productId")
        if t == "promoFull" and isinstance(pid, list):
            for code in pid:
                clone = dict(it)
                clone["productId"] = str(code)
                yield clone
        else:
            yield it

def _handle_message(body: bytes):
    try:
        doc = json.loads(body)
    except Exception as e:

        send_to_dlq({"raw": body.decode("utf-8", "ignore")}, f"invalid JSON: {e}", stage="ingest")
        return

    ok_count, bad_count = 0, 0
    for item in _split_items(doc):
        try:
            msg = process_item(doc, item)
            if msg is None:
                bad_count += 1
                continue

            ok_count += 1

        except Exception as e:
            bad_count += 1
            err = "".join(traceback.format_exception_only(type(e), e)).strip()
            problem = {"doc_meta": {k: doc.get(k) for k in ("provider","branch","type","timestamp")},
                       "item": item}
            send_to_dlq(problem, f"processing error: {err}", stage="process")

    print(f"[worker] processed doc: ok={ok_count} bad={bad_count}", flush=True)

def main():
    conn, ch = _connect()

    def _cb(ch_, method, properties, body):
        try:
            _handle_message(body)
            ch_.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            try:
                send_to_dlq({"raw": body.decode("utf-8","ignore")}, "fatal handler error", stage="fatal")
            except Exception:
                pass
            ch_.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=INPUT_QUEUE, on_message_callback=_cb, auto_ack=False)
    print(f"[worker] listening on {INPUT_QUEUE} ...", flush=True)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            ch.stop_consuming()
        except Exception:
            pass
        conn.close()

if __name__ == "__main__":
    main()
