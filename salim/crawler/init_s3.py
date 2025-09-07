import os, boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION      = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack:4566")
BUCKET      = os.getenv("S3_BUCKET", "test-bucket")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    region_name=REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    config=Config(s3={"addressing_style": "path"}),
)

try:
    s3.head_bucket(Bucket=BUCKET)
    print(f"[s3] bucket exists: {BUCKET}")
except ClientError:
    s3.create_bucket(Bucket=BUCKET)
    print(f"[s3] bucket created: {BUCKET}")