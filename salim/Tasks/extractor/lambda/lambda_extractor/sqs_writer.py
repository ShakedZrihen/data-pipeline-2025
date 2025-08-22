
import json
import boto3
import config

class sqs_writer :

    def __init__(self):
        self._sqs = boto3.client("sqs",
            endpoint_url=config.ENDPOINT_URL,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=config.AWS_REGION)

    def send_pointer(self, bucket: str, key: str):
        """Send pointer (S3 bucket + key) as SQS message."""
        _sqs = boto3.client("sqs",
            endpoint_url=config.ENDPOINT_URL,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=config.AWS_REGION)

        msg = {"bucket": bucket, "key": key}
        _sqs.send_message(
            QueueUrl=config.QUEUE_URL,
            MessageBody=json.dumps(msg, ensure_ascii=False)
        )
        return msg
