import os, pathlib, boto3
from botocore.config import Config

REGION      = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack:4566")
BUCKET      = os.getenv("S3_BUCKET", "test-bucket")
ROOT_DIR    = pathlib.Path(os.getenv("CRAWLER_OUTPUT_DIR", "/app/crawler_data"))

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    region_name=REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    config=Config(s3={"addressing_style": "path"}),
)

def upload_all_files_to_localstack():
    for p in ROOT_DIR.rglob("*.gz"):
        key = str(p.relative_to(ROOT_DIR)).replace("\\", "/")
        print(f"[upload] {p} -> s3://{BUCKET}/{key}")
        s3.upload_file(str(p), BUCKET, key)

if __name__ == "__main__":
    upload_all_files_to_localstack()
