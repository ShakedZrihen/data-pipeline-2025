# message_producer.py (updated, backward‑compatible)
# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import logging
import os
import time
import hashlib
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# RabbitMQ optional support (TRANSPORT=rabbitmq)
try:
    import pika  # type: ignore
except Exception:  # pragma: no cover
    pika = None


class MessageProducer:
    """
    Backward‑compatible message producer for SQS or RabbitMQ.

    Additions (non‑breaking):
      - skip_if_empty (optional): if True, do not publish known‑empty payloads
        (e.g., items=[], items_emitted==0). Can also be toggled via env var
        SKIP_EMPTY_MESSAGES=1.
      - FIFO support: if the queue URL ends with .fifo, we auto‑set
        MessageGroupId (derived from message fields or provided explicitly) and a
        content hash as MessageDeduplicationId (safe even if ContentBasedDedup is on).
      - Light MessageAttributes (provider/branch/type/ts) when present in message.
    """

    def __init__(
        self,
        transport: str = "sqs",
        sqs_queue_url: Optional[str] = None,
        rabbitmq_url: Optional[str] = None,
        rabbitmq_exchange: str = "",
        rabbitmq_routing_key: str = "prices.events",
        *,
        skip_if_empty: Optional[bool] = None,
        fifo_group_id: Optional[str] = None,
        enable_fifo_autodedup: bool = True,
    ):
        self.transport = (transport or "sqs").lower()
        self.sqs_queue_url = sqs_queue_url
        self._sqs = boto3.client("sqs") if self.transport == "sqs" else None

        self.rabbitmq_url = rabbitmq_url
        self.rabbitmq_exchange = rabbitmq_exchange
        self.rabbitmq_routing_key = rabbitmq_routing_key
        self._rabbit_conn = None
        self._rabbit_channel = None

        if self.transport == "rabbitmq":
            if not pika:
                raise RuntimeError("TRANSPORT=rabbitmq but 'pika' not installed")
            if not rabbitmq_url:
                raise RuntimeError("RABBITMQ_URL is required for RabbitMQ transport")
            self._rabbit_conn = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
            self._rabbit_channel = self._rabbit_conn.channel()

        if self.transport == "sqs" and not self.sqs_queue_url:
            raise RuntimeError("SQS_QUEUE_URL is required for SQS transport")

        # New, optional behavior (off by default unless env set)
        if skip_if_empty is None:
            env_val = os.getenv("SKIP_EMPTY_MESSAGES", "").strip().lower()
            skip_if_empty = env_val in {"1", "true", "yes", "on"}
        self.skip_if_empty = bool(skip_if_empty)

        # FIFO detection and options
        self._is_fifo = bool(self.sqs_queue_url and self.sqs_queue_url.endswith(".fifo"))
        self.fifo_group_id = fifo_group_id  # optional explicit group id
        self.enable_fifo_autodedup = bool(enable_fifo_autodedup)

    # ----------------------------- public API -----------------------------

    def send(self, message: Dict[str, Any]):
        body = json.dumps(message, ensure_ascii=False)

        # Optional safety – never breaks existing flows because default is False
        if self.skip_if_empty and self._looks_empty(message, body):
            logger.warning("Skipping publish: empty payload (per policy)")
            return

        if self.transport == "sqs":
            size = len(body.encode("utf-8"))
            if size >= 250 * 1024:
                logger.warning("SQS body is near the 256KB limit (%d bytes)", size)
            params: Dict[str, Any] = {
                "QueueUrl": self.sqs_queue_url,
                "MessageBody": body,
            }

            # Message attributes (harmless if consumer ignores them)
            attrs = {}
            for k in ("provider", "branch", "type", "timestamp", "ts"):
                v = message.get(k)
                if isinstance(v, (str, int, float)) and str(v):
                    attrs[k] = {"DataType": "String", "StringValue": str(v)}
            if attrs:
                params["MessageAttributes"] = attrs

            # FIFO niceties
            if self._is_fifo:
                gid = self.fifo_group_id or self._derive_group_id(message) or "default"
                params["MessageGroupId"] = gid
                if self.enable_fifo_autodedup:
                    params["MessageDeduplicationId"] = hashlib.sha256(body.encode("utf-8")).hexdigest()

            logger.info("Sending to SQS (%d bytes) -> %s", size, self.sqs_queue_url)
            self._send_sqs_with_retries(params)
        else:
            logger.info(
                "Publishing to RabbitMQ exchange=%s routing_key=%s",
                self.rabbitmq_exchange,
                self.rabbitmq_routing_key,
            )
            self._rabbit_channel.basic_publish(
                exchange=self.rabbitmq_exchange,
                routing_key=self.rabbitmq_routing_key,
                body=body.encode("utf-8"),
                properties=None,
                mandatory=False,
            )

    # ----------------------------- internals -----------------------------

    def _derive_group_id(self, message: Dict[str, Any]) -> Optional[str]:
        parts = [
            str(message.get("provider", "")).strip(),
            str(message.get("branch", "")).strip(),
            str(message.get("type", "")).strip(),
        ]
        parts = [p for p in parts if p]
        return "|".join(parts) if parts else None

    def _looks_empty(self, msg: Dict[str, Any], body: str) -> bool:
        # Common shapes in this project
        try:
            if isinstance(msg.get("items"), list) and len(msg["items"]) == 0:
                return True
            for stats_key in ("stats", "metadata"):
                stats = msg.get(stats_key) or {}
                if isinstance(stats, dict) and stats.get("items_emitted") == 0:
                    return True
        except Exception:
            pass
        # Obvious minimal payloads
        if body.strip() in ("[]", "{}"):
            return True
        return False

    def _send_sqs_with_retries(self, params: Dict[str, Any], attempts: int = 3, backoff: float = 0.3) -> None:
        last_err: Optional[Exception] = None
        for i in range(1, attempts + 1):
            try:
                self._sqs.send_message(**params)
                return
            except (ClientError, BotoCoreError) as e:
                last_err = e
                if i < attempts:
                    time.sleep(backoff * i)
                else:
                    logger.error("Failed to send SQS message after %d attempts: %s", attempts, e)
                    raise

    def __del__(self):
        try:
            if self._rabbit_conn:
                self._rabbit_conn.close()
        except Exception:
            pass


