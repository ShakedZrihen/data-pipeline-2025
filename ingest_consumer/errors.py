import os, json, uuid, boto3

def _sqs():
    return boto3.client(
        "sqs",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )

def _s3():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )

def _dlq_url():
    url = os.getenv("DLQ_QUEUE_URL")
    if not url:
        raise RuntimeError("DLQ_QUEUE_URL is not set")
    return url

DLQ_BUCKET = os.getenv("DLQ_BUCKET", "price-updates")

def send_to_dlq(original_body: str, error_msg: str):
    s3 = _s3()
    try:
        s3.create_bucket(Bucket=DLQ_BUCKET)
    except Exception:
        pass

    key = f"dlq/{uuid.uuid4()}.json"
    s3.put_object(Bucket=DLQ_BUCKET, Key=key, Body=original_body.encode("utf-8"), ContentType="application/json")

    payload = {"error": error_msg, "s3_bucket": DLQ_BUCKET, "s3_key": key}
    msg = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    _sqs().send_message(QueueUrl=_dlq_url(), MessageBody=msg)
