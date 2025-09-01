# enricher/main.py
import json
from sqs_consumer import SQSConsumer
from sys import stdout


def handle(body: str) -> bool:
    try:
        payload = json.loads(body)
    except Exception as e:
        print(f"[WARN] invalid JSON: {e}; dropping")
        return True  # ack to avoid poison-pill loops

    items = payload.get("items") or []
    print(
        "[MSG]",
        f"provider={payload.get('provider')!r}",
        f"branch={payload.get('branch')!r}",
        f"type={payload.get('type')!r}",
        f"items={len(items)}",
    )
    return True  # ack


if __name__ == "__main__":
    try:
        stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    SQSConsumer().poll(handle)
