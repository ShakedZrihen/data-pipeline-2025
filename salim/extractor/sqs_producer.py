import os, json, boto3

class SQSProducer:
    def __init__(self, queue_url: str | None):
        if not queue_url:
            raise ValueError("SQS_QUEUE_URL env var is missing")
        self.queue_url = queue_url
        self.sqs = boto3.client(
            "sqs",
            endpoint_url=os.getenv("S3_ENDPOINT_URL", os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )

    def send(self, data: dict):
        body = json.dumps(data, ensure_ascii=False)
        resp = self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=body)
        print("SQS MessageId:", resp.get("MessageId"))
