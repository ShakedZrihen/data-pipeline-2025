import os
from dotenv import load_dotenv

load_dotenv()

SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/my-queue")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
WAIT_TIME_SECONDS = int(os.getenv("WAIT_TIME_SECONDS", "1"))
REGION_NAME = os.getenv("AWS_REGION", "us-east-1")
DLQ_URL = os.getenv("DLQ_URL", "http://localhost:4566/000000000000/dlq")
