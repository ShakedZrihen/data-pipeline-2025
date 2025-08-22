
import os

# Endpoint for LocalStack (inside Lambda container use the container DNS name)
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://localhost:4566")         # for aws CLI
LAMBDA_ENDPOINT_IN_CONTAINER="http://localstack:4566"
AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET    = os.getenv("S3_BUCKET", "providers-bucket")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "providers-events")
DDB_TABLE    = os.getenv("DDB_TABLE", "LastRuns")
OUTPUT_JSON_PREFIX = os.getenv("OUTPUT_JSON_PREFIX", "Json/")
LOG_TO_S3    = os.getenv("LOG_TO_S3", "0") == "1"
LOG_BUCKET   = os.getenv("LOG_BUCKET", S3_BUCKET)
QUEUE_URL = os.getenv("QUEUE_URL", f"{ENDPOINT_URL}/000000000000/{SQS_QUEUE_NAME}")

# Supported filetypes
SUPPORTED_TYPES = {"pricesFull", "promoFull"}
