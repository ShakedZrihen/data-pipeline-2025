# enricher/sqs_consumer.py
import os
import json
import time
import boto3
from typing import Callable, Optional


def _int_env(
    name: str, default: int, *, clamp: Optional[tuple[int, int]] = None
) -> int:
    try:
        v = int(os.getenv(name, str(default)))
    except ValueError:
        v = default
    if clamp:
        lo, hi = clamp
        v = max(lo, min(hi, v))
    return v


class SQSConsumer:
    def __init__(self):
        self.queue_url = os.getenv(
            "SQS_QUEUE_URL", "http://localstack:4566/000000000000/sqs-queue"
        )
        region = os.getenv("AWS_REGION", "us-east-1")
        endpoint = os.getenv("SQS_ENDPOINT_URL")
        kwargs = {"region_name": region}
        if endpoint:
            kwargs["endpoint_url"] = endpoint

        self.sqs = boto3.client(
            "sqs",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            **kwargs,
        )

        self.wait_time_seconds = _int_env("SQS_WAIT_TIME_SECONDS", 10, clamp=(0, 20))
        self.max_messages = _int_env("SQS_MAX_MESSAGES", 10, clamp=(1, 10))
        self.visibility_timeout = _int_env("SQS_VISIBILITY_TIMEOUT", 60)

    def poll(self, handle_message: Callable[[str], bool]) -> None:
        """Long-poll, call handler per message; delete only on True."""
        try:
            while True:
                resp = self.sqs.receive_message(
                    QueueUrl=self.queue_url,
                    MaxNumberOfMessages=self.max_messages,
                    WaitTimeSeconds=self.wait_time_seconds,
                    VisibilityTimeout=self.visibility_timeout,
                )
                msgs = resp.get("Messages", [])
                if not msgs:
                    continue

                to_delete = []
                for m in msgs:
                    body = m.get("Body", "")
                    try:
                        ok = handle_message(body)
                    except Exception as e:
                        print(f"[ERROR] handler exception: {e!r}")
                        ok = False

                    if ok:
                        to_delete.append(
                            {
                                "Id": m["MessageId"],
                                "ReceiptHandle": m["ReceiptHandle"],
                            }
                        )

                if to_delete:
                    self.sqs.delete_message_batch(
                        QueueUrl=self.queue_url, Entries=to_delete
                    )
        except KeyboardInterrupt:
            print("SQS consumer stopped.")
        except Exception as e:
            print(f"[FATAL] SQS polling failed: {e!r}")
            time.sleep(2)
            raise
