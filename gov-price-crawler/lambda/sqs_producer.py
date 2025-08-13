# sqs_producer.py
import json
import logging
from typing import Any, Dict, List

import boto3

logger = logging.getLogger(__name__)
sqs = boto3.client("sqs")

MAX_MSG_BYTES = 256_000  # keep under 262,144

def send_envelope(envelope_with_items: Dict[str, Any], queue_url: str) -> int:
    items: List[Dict[str, Any]] = envelope_with_items.get("items") or []
    base = {k: v for k, v in envelope_with_items.items() if k != "items"}

    if not queue_url:
        logger.warning("QUEUE_URL empty; not sending to SQS. (Preview below)")
        preview = dict(base, items=items[:5])
        logger.info("Envelope preview: %s ... (total items=%d)", json.dumps(preview, ensure_ascii=False), len(items))
        return 0

    if not items:
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(dict(base, items=[]), ensure_ascii=False))
        return 1

    sent = 0
    chunk: List[Dict[str, Any]] = []
    for it in items:
        trial = json.dumps(dict(base, items=chunk + [it]), ensure_ascii=False)
        if len(trial.encode("utf-8")) > MAX_MSG_BYTES:
            if chunk:
                sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(dict(base, items=chunk), ensure_ascii=False))
                sent += 1
                chunk = [it]
            else:
                logger.warning("Single item too large, dropping: %s", str(it)[:200])
        else:
            chunk.append(it)

    if chunk:
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(dict(base, items=chunk), ensure_ascii=False))
        sent += 1

    return sent
