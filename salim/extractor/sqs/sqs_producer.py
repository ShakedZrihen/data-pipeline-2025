import os
import sys
import json
import boto3
from botocore.exceptions import ClientError

SQS_LIMIT_BYTES = 262_144  # hard SQS body limit

def endpoint_url():
    return (
        os.getenv("SQS_ENDPOINT_URL")
        or os.getenv("SQS_ENDPOINT")
        or "http://localhost:4566"
    )

def region():
    return os.getenv("AWS_DEFAULT_REGION", "us-east-1")

def aws_key():
    return os.getenv("AWS_ACCESS_KEY_ID", "test")

def aws_secret():
    return os.getenv("AWS_SECRET_ACCESS_KEY", "test")

def queue_name():
    return os.getenv("SQS_QUEUE_NAME", "test-queue")

def get_client():
    return boto3.client(
        "sqs",
        endpoint_url=endpoint_url(),
        aws_access_key_id=aws_key(),
        aws_secret_access_key=aws_secret(),
        region_name=region(),
    )

def get_or_create_queue_url(sqs_client, name):
    try:
        return sqs_client.get_queue_url(QueueName=name)["QueueUrl"]
    except sqs_client.exceptions.QueueDoesNotExist:
        resp = sqs_client.create_queue(QueueName=name)
        return resp["QueueUrl"]

def send_message_to_sqs(message_body):
    if not isinstance(message_body, str):
        message_body = json.dumps(message_body, ensure_ascii=False)

    body_bytes = message_body.encode("utf-8")
    byte_len = len(body_bytes)

    preview = message_body[:300].replace("\n", " ")
    print(f"[SQS] Prepared body bytes={byte_len}  preview='{preview}...'")

    if byte_len >= SQS_LIMIT_BYTES:
        print(f"[SQS] Body too large ({byte_len} >= {SQS_LIMIT_BYTES}) — not sending.")
        return {"ok": False, "error": "body_too_large", "bytes": byte_len}

    sqs_client = get_client()
    queue_url = os.getenv("SQS_QUEUE_URL")
    if not queue_url:
        queue_url = get_or_create_queue_url(sqs_client, queue_name())
    print(f"Queue URL: {queue_url}")

    try:
        response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=message_body)
        print("✅ Message sent successfully!")
        print(f"   Message ID: {response.get('MessageId')}")
        if "MD5OfMessageBody" in response:
            print(f"   MD5 of Body: {response['MD5OfMessageBody']}")

        try:
            attrs = sqs_client.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
            )
            print(f"   Messages in queue: {attrs['Attributes'].get('ApproximateNumberOfMessages', '0')}")
        except Exception:
            pass

        return {"ok": True, "message_id": response.get("MessageId"), "bytes": byte_len}

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        print(f"[SQS] Error ({code}): {e}")
        if code == "InvalidParameterValue":
            print(f"[SQS] Hint: body exceeded {SQS_LIMIT_BYTES} bytes?")
        if code == "AWS.SimpleQueueService.NonExistentQueue":
            print(f"[SQS] Queue '{queue_name()}' does not exist. Is LocalStack up?")
        return {"ok": False, "error": code, "bytes": byte_len}
    except Exception as e:
        print(f"[SQS] Unexpected error: {e}")
        return {"ok": False, "error": "unexpected", "bytes": byte_len}

def receive_messages_from_sqs():
    print("Receiving messages from SQS queue...")

    sqs_client = get_client()
    name = queue_name()

    try:
        queue_url = get_or_create_queue_url(sqs_client, name)

        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=int(os.getenv("SQS_MAX_PER_POLL", "10")),
            WaitTimeSeconds=int(os.getenv("SQS_WAIT_TIME", "5")),
        )
        msgs = resp.get("Messages", [])
        if msgs:
            print(f"📥 Received {len(msgs)} messages:")
            for i, m in enumerate(msgs, 1):
                print(f"   Message {i}:")
                print(f"     ID: {m['MessageId']}")
                print(f"     Body: {m['Body'][:500]}...")
                print(f"     Receipt Handle: {m['ReceiptHandle'][:20]}...")
                print()
        else:
            print("📭 No messages available in queue")

    except ClientError as e:
        print(f"Error receiving messages: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
