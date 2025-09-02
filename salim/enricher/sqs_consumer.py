import os
import time
import logging
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


class SQSConsumer:
    def __init__(self):
        self.queue_spec = os.getenv("SQS_QUEUE_URL", "").strip()
        if not self.queue_spec:
            raise ValueError("SQS_QUEUE_URL is required (queue name or full URL)")

        region = os.getenv("AWS_REGION", "us-east-1")
        endpoint = (
            os.getenv("SQS_ENDPOINT_URL")
            or os.getenv("LOCALSTACK_ENDPOINT")
            or os.getenv("S3_ENDPOINT_URL")
        )

        self.wait_time_seconds = min(
            int(os.getenv("SQS_WAIT_TIME_SECONDS", "10")), 20
        )  # SQS max 20
        self.max_messages = max(1, min(int(os.getenv("SQS_MAX_MESSAGES", "10")), 10))
        self.visibility_timeout = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "60"))
        self.empty_sleep = float(os.getenv("SQS_EMPTY_SLEEP", "2"))

        self.sqs = boto3.client(
            "sqs",
            region_name=region,
            endpoint_url=endpoint,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        )

        # If user gave a URL, use it. If they gave a name, resolve to URL (no creation).
        self.queue_url = self.queue_spec if "://" in self.queue_spec else None
        self.queue_name = self._queue_name_from_spec(self.queue_spec)

        if not self.queue_url:
            self._try_resolve_url()  # ok if it fails; we'll keep retrying in poll()

        log.info(
            "SQS consumer init | endpoint=%s | spec=%s | url=%s",
            getattr(self.sqs, "_endpoint", None) and self.sqs._endpoint.host,
            self.queue_spec,
            self.queue_url,
        )

    @staticmethod
    def _queue_name_from_spec(spec: str) -> str:
        parsed = urlparse(spec)
        return parsed.path.rsplit("/", 1)[-1] if parsed.scheme else spec

    def _try_resolve_url(self) -> bool:
        """Try to resolve a queue name to a URL. Never creates."""
        try:
            resp = self.sqs.get_queue_url(QueueName=self.queue_name)
            self.queue_url = resp["QueueUrl"]
            log.info("Resolved queue URL: %s", self.queue_url)
            return True
        except self.sqs.exceptions.QueueDoesNotExist:
            return False
        except ClientError:
            return False

    def poll(self, handle_message):
        """Keep polling the queue. If queue missing, wait and retry."""
        backoff = 1.0

        while True:
            # If we don’t have a URL yet (only a name), keep trying to resolve.
            if not self.queue_url:
                if self._try_resolve_url():
                    backoff = 1.0
                else:
                    log.info("Queue '%s' not found yet; waiting...", self.queue_name)
                    time.sleep(min(backoff, 30))
                    backoff = min(backoff * 1.5, 30)
                continue

            try:
                resp = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=self.max_messages,
                    WaitTimeSeconds=self.wait_time_seconds,
                    VisibilityTimeout=self.visibility_timeout,
                )
                msgs = resp.get("Messages", [])
                if not msgs:
                    time.sleep(self.empty_sleep)
                    backoff = 1.0
                    continue

                to_delete = []
                for m in msgs:
                    ok = False
                    try:
                        ok = handle_message(m.get("Body", ""))
                    except Exception as e:
                        log.exception("Handler error: %r", e)

                    if ok:
                        to_delete.append(
                            {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]}
                        )

                if to_delete:
                    self.sqs.delete_message_batch(
                        QueueUrl=self.queue_url, Entries=to_delete
                    )

            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in (
                    "AWS.SimpleQueueService.NonExistentQueue",
                    "QueueDoesNotExist",
                    "InvalidAddress",
                ):
                    # Queue was not created yet or was deleted. Reset URL & wait.
                    log.warning("Queue not available (%s). Will wait and retry…", code)
                    self.queue_url = None
                    time.sleep(min(backoff, 30))
                    backoff = min(backoff * 1.5, 30)
                    continue

                log.error("SQS receive failed: %s", e, exc_info=True)
                time.sleep(min(backoff, 30))
                backoff = min(backoff * 1.5, 30)
