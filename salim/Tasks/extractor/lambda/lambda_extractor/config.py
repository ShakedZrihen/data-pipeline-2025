# extractor/config.py
import os


ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://s3-simulator:4566")
AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET    = os.getenv("S3_BUCKET", "providers-bucket")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "providers-events")
DDB_TABLE    = os.getenv("DDB_TABLE", "LastRuns")
OUTPUT_JSON_PREFIX = os.getenv("OUTPUT_JSON_PREFIX", "Json/")
LOG_TO_S3    = os.getenv("LOG_TO_S3", "0") == "1"
LOG_BUCKET   = os.getenv("LOG_BUCKET", S3_BUCKET)
# Toggle LocalStack (kept for parity; True in your local env)
LOCALSTACK = os.getenv("LOCALSTACK", "1") == "1"

# LocalStack endpoint when LOCALSTACK=1
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "http://localhost:4566") if LOCALSTACK else None

# Supported filetypes (from your task)
SUPPORTED_TYPES = {"pricesFull", "promoFull"}
