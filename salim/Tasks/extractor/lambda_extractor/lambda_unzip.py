
import boto3
import config
from unZip import UnZip   # keep your original functions
from s3_writer import write_payload_json
from sqs_writer import sqs_writer


class GzExtractorLambda:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=config.ENDPOINT_URL,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=config.AWS_REGION,
        )

    def handle(self, event, context):
        """Triggered by S3 when a new .gz file is uploaded"""
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            print(f"New upload detected: s3://{bucket}/{key}")

            # Reuse your existing unzipper
            print(f"Reading and unzipping {key} from bucket {bucket}...")
            payload = UnZip._read_and_unzip(self.s3, bucket, key)
            # print(f"Extracted payload: {payload}")
            if not payload:
                continue

            # Reuse your normalizer + writer
            json_key = write_payload_json(payload)
            print(f"Wrote normalized JSON to s3://{bucket}/{json_key}")

            # Send pointer to SQS
            writer = sqs_writer()
            writer.send_pointer(bucket, json_key)
            print(f"Sent pointer to queue: {bucket}/{json_key}")


# Lambda entrypoint (what AWS/LocalStack will call)
def handler(event, context):
    return GzExtractorLambda().handle(event, context)
