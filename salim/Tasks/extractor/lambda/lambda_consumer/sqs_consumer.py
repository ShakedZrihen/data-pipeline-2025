
import json, os, time, traceback
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from lambda_extractor import config

class SqsS3Consumer:
    def __init__(
        self,
        queue_url: Optional[str] = None,
        s3_endpoint: Optional[str] = None,
        sqs_endpoint: Optional[str] = None,
        region_name: Optional[str] = None,
        visibility_timeout: int = 60,
        wait_time_seconds: int = 20,
        batch_size: int = 10,
    ):

        self.queue_url = config.QUEUE_URL
        self.region_name = region_name or config.AWS_REGION
        # On host: S3/SQS endpoints should be http://localhost:4566
        self.s3 = boto3.client(
            "s3",
            endpoint_url=(s3_endpoint or config.ENDPOINT_URL or "http://localhost:4566"),
            aws_access_key_id="test", aws_secret_access_key="test", region_name=self.region_name,
        )
        self.sqs = boto3.client(
            "sqs",
            endpoint_url=(sqs_endpoint or os.getenv("ENDPOINT_URL", "http://localhost:4566")),
            aws_access_key_id="test", aws_secret_access_key="test", region_name=self.region_name,
        )
        self.visibility_timeout = visibility_timeout
        self.wait_time_seconds = wait_time_seconds
        self.batch_size = max(1, min(batch_size, 10))

    def process_message(self, body: Dict[str, Any]) -> None:
        bucket, key = body["bucket"], body["key"]
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
        print(f"[OK] {bucket}/{key} ({len(content)} bytes)")
        print(f"Content preview: {content[:100]}...")  # Show first 100 chars for preview
        # TODO: your DB insert / transform here

    def _receive_batch(self) -> List[Dict[str, Any]]:
        try:
            resp = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.batch_size,
                WaitTimeSeconds=self.wait_time_seconds,   # long poll
                VisibilityTimeout=self.visibility_timeout # time to process before it reappears
            )
            return resp.get("Messages", []) or []
        except (ClientError, BotoCoreError) as e:
            print(f"[recv] {e}"); time.sleep(1); return []

    def _delete(self, receipt_handle: str) -> None:
        try:
            self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except (ClientError, BotoCoreError) as e:
            print(f"[del ] {e}")

    def run_forever(self) -> None:
        print(f"[run ] polling {self.queue_url} …")
        while True:
            msgs = self._receive_batch()
            if not msgs:
                continue
            for m in msgs:
                try:
                    body = json.loads(m["Body"])
                    self.process_message(body)
                    self._delete(m["ReceiptHandle"])
                except Exception:
                    print("[ERR ] processing failed; leaving message for retry")
                    traceback.print_exc()
            time.sleep(0.1)

if __name__ == "__main__":
    SqsS3Consumer().run_forever()
