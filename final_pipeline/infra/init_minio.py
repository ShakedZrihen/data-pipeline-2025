import os, time
import boto3
from botocore.client import Config
import pika

S3_ENDPOINT=os.getenv("S3_ENDPOINT")
S3_BUCKET=os.getenv("S3_BUCKET")
S3_ACCESS_KEY=os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY=os.getenv("S3_SECRET_KEY")

RABBITMQ_HOST=os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER=os.getenv("RABBITMQ_DEFAULT_USER", "guest")
RABBITMQ_PASS=os.getenv("RABBITMQ_DEFAULT_PASS", "guest")
RABBITMQ_QUEUE=os.getenv("RABBITMQ_QUEUE", "prices_queue")

def wait_for_minio():
    for _ in range(60):
        try:
            s3 = boto3.client("s3",
                              endpoint_url=S3_ENDPOINT,
                              aws_access_key_id=S3_ACCESS_KEY,
                              aws_secret_access_key=S3_SECRET_KEY,
                              region_name="us-east-1",
                              config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"))
            s3.list_buckets()
            return s3
        except Exception as e:
            time.sleep(2)
    raise SystemExit("MinIO not ready")

def ensure_bucket(s3):
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if S3_BUCKET not in buckets:
        s3.create_bucket(Bucket=S3_BUCKET)

def ensure_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    for _ in range(60):
        try:
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            conn.close()
            return
        except Exception:
            time.sleep(2)
    raise SystemExit("RabbitMQ not ready")

if __name__ == "__main__":
    s3 = wait_for_minio()
    ensure_bucket(s3)
    ensure_rabbitmq()
    print("Infra initialization complete.")
