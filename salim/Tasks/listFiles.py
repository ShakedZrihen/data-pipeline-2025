import boto3
import os

ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "providers-bucket")

s3 = boto3.client("s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=AWS_REGION
)

def list_files(prefix=""):
    """List all files in bucket under an optional prefix, and print contents."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            print(f"--- {key} ---")

            # Fetch file content
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            body = response["Body"].read()

            # Try to decode if text, otherwise show raw bytes length
            try:
                print(body.decode("utf-8")[:500])  # first 500 chars for preview
            except UnicodeDecodeError:
                print(f"[Binary content: {len(body)} bytes]")

if __name__ == "__main__":
    list_files("Json/")
    # list_files("providers/")
