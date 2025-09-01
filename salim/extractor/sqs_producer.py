import os, json, boto3

SQS_LIMIT_BYTES = 225_000

class SQSProducer:
    def __init__(self, queue_url: str | None):
        self.queue_url = queue_url or os.environ.get("SQS_QUEUE_URL")
        if not self.queue_url:
            raise ValueError("SQS_QUEUE_URL env var is missing")

        endpoint = os.environ.get("LOCALSTACK_ENDPOINT")
        region = os.environ.get("AWS_REGION", "us-east-1")

        kwargs = {"region_name": region}
        if endpoint:
            kwargs["endpoint_url"] = endpoint

        self.sqs = boto3.client(
            "sqs",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            **kwargs,
        )

    def send(self, data: dict):
        self.send_json(data)

    def send_json(self, payload: dict):
        """Send payload to SQS; chunk on items if the message is too large."""
        print("Preparing SQS message ")
        body = json.dumps(payload, ensure_ascii=False)
        if len(body.encode("utf-8")) <= SQS_LIMIT_BYTES:
            return self._send_body(body)

        # Split by items
        base = dict(payload)
        items = base.pop("items", [])
        if not isinstance(items, list):
            return self._send_body(body)

        base_bytes = len(json.dumps(base, ensure_ascii=False).encode("utf-8")) + 30
        batches, cur, cur_size = [], [], base_bytes

        for it in items:
            it_s = json.dumps(it, ensure_ascii=False)
            it_b = len(it_s.encode("utf-8")) + 1
            if cur and cur_size + it_b > SQS_LIMIT_BYTES:
                batches.append(cur)
                cur, cur_size = [], base_bytes
            cur.append(it)
            cur_size += it_b
        if cur:
            batches.append(cur)

        total_parts = len(batches)
        for idx, batch in enumerate(batches, start=1):
            msg = {**base, "items": batch}
            if total_parts > 1:
                msg["part"] = idx
                msg["parts"] = total_parts
            self._send_body(json.dumps(msg, ensure_ascii=False))

    def _send_body(self, body: str):
        print(f"Sending SQS message ({len(body.encode('utf-8'))} bytes) , {self.queue_url}")
        resp = self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=body)
        print("SQS MessageId:", resp.get("MessageId"))
