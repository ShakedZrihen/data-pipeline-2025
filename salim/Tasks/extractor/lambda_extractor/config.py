
import os


ENDPOINT_URL = (
    os.getenv("AWS_ENDPOINT_URL")    # standard   # fallback
    or "http://localstack:4566"
)
AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET    = os.getenv("S3_BUCKET", "providers-bucket")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "providers-events")
QUEUE_URL = os.getenv("QUEUE_URL", f"{ENDPOINT_URL}/000000000000/{SQS_QUEUE_NAME}")

# Supported filetypes
SUPPORTED_TYPES = {"pricesFull", "promoFull"}
