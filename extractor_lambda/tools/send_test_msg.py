import os, sys, json, boto3

def main():

    queue_url = os.environ.get("SQS_QUEUE_URL") or os.environ.get("OUTPUT_QUEUE_URL")
    if not queue_url:
        raise SystemExit("Please set SQS_QUEUE_URL or OUTPUT_QUEUE_URL in your env.")

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    region = os.environ.get("AWS_REGION", "us-east-1")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")

    sqs = boto3.client(
        "sqs",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    if len(sys.argv) > 1:
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            body_obj = json.load(f)
    else:
        body_obj = {
            "provider": "Keshet",
            "branch": "104",
            "type": "pricesFull",
            "timestamp": "2025-09-01T06:30:00Z",
            "items_total": 2,
            "items_sample": [
                {"product": "חלב תנובה 3%", "price": 5.9, "unit": "liter"},
                {"product": "לחם פרוס", "price": 9.9, "unit": "unit"},
            ],
        }

    body_str = json.dumps(body_obj, ensure_ascii=False, separators=(",", ":"))

    resp = sqs.send_message(QueueUrl=queue_url, MessageBody=body_str)
    print(" Sent message", resp["MessageId"])

if __name__ == "__main__":
    main()
