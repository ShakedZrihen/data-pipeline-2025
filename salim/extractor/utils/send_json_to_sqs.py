import boto3
import os
import json
import sys

MAX_SQS_SIZE = 256_000


def send_items_in_chunks(target_dict, limit=50):
    root = target_dict.get("root") or target_dict.get("Root")
    if not root or "items" not in root:
        raise ValueError("No 'items' key in root – cannot chunk.")

    base = {k: v for k, v in root.items() if k != "items"}
    items = root["items"]

    chunks_sent = 0

    for item in items:
        if chunks_sent >= limit:
            print(f"Reached chunk limit of {limit}. Stopping.")
            break

        chunk = {
            "root": {
                **base,
                "items": [item]
            }
        }

        encoded = json.dumps(chunk).encode("utf-8")
        if len(encoded) > 256000:
            print("One item too large to send to SQS. Skipping.")
            continue

        _send_json_to_sqs(chunk)
        chunks_sent += 1



def send_promotions_in_chunks(target_dict):
    root = target_dict.get("root") or target_dict.get("Root")
    if not root or "promotions" not in root:
        raise ValueError("No 'promotions' key in root - cannot chunk.")

    base = {k: v for k, v in root.items() if k != "promotions"}
    promotions = root["promotions"]

    for promo in promotions:
        chunk = {
            "root": {
                **base,
                "promotions": [promo]
            }
        }

        encoded = json.dumps(chunk).encode("utf-8")
        if len(encoded) > 256000:
            print("One promo too large to send to SQS. Skipping.")
            continue

        _send_json_to_sqs(chunk)



def _send_json_to_sqs(json_data):
    """
    Sends the JSON content to SQS as a MessageBody.
    json_data can be a dict or a JSON string.
    """
    if isinstance(json_data, dict):
        json_str = json.dumps(json_data)
    elif isinstance(json_data, str):
        json_str = json_data
    else:
        raise ValueError("json_data must be a dict or JSON string")

    sqs_client = boto3.client(
        'sqs',
        endpoint_url=os.getenv('SQS_ENDPOINT', 'http://localhost:4566'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )

    queue_url = os.getenv('SQS_QUEUE_URL', 'http://localhost:4566/000000000000/my-queue')

    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json_str
        )
        print(f"Sent message to SQS. MessageId: {response['MessageId']}")
    except Exception as e:
        print(f"Failed to send message to SQS: {e}")
