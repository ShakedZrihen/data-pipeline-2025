import os
import sys
import json
import boto3
from botocore.exceptions import ClientError

SQS_LIMIT_BYTES = 262_144  # hard SQS body limit

def send_message_to_sqs(message_body):
    """Send a message to SQS queue using LocalStack"""

    # 1) Ensure string once (no double-dumps)
    if not isinstance(message_body, str):
        message_body = json.dumps(message_body, ensure_ascii=False)

    body_bytes = message_body.encode("utf-8")
    byte_len = len(body_bytes)

    # Safer logging (avoid dumping whole body)
    preview = message_body[:300].replace("\n", " ")
    print(f"[SQS] Prepared body bytes={byte_len}  preview='{preview}...'")

    # 2) Optional guard: don’t even call SQS if over the limit
    if byte_len >= SQS_LIMIT_BYTES:
        print(f"[SQS] ⚠️ Body is too large ({byte_len} bytes >= {SQS_LIMIT_BYTES}). Not sending.")
        # Return a structured result instead of exiting the process
        return {"ok": False, "error": "body_too_large", "bytes": byte_len}

    # 3) Client + queue URL (allow env overrides)
    sqs_client = boto3.client(
        "sqs",
        endpoint_url=os.getenv("SQS_ENDPOINT", "http://localhost:4567"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    queue_name = os.getenv("SQS_QUEUE_NAME", "test-queue")
    try:
        queue_url = os.getenv("SQS_QUEUE_URL")
        if not queue_url:
            queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        print(f"Queue URL: {queue_url}")

        # 4) Send (no MessageAttributes to avoid extra bytes)
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )

        print("✅ Message sent successfully!")
        print(f"   Message ID: {response.get('MessageId')}")
        if "MD5OfMessageBody" in response:
            print(f"   MD5 of Body: {response['MD5OfMessageBody']}")

        # 5) Optional: show queue depth
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        print(f"   Messages in queue: {attrs['Attributes'].get('ApproximateNumberOfMessages', '0')}")
        return {"ok": True, "message_id": response.get("MessageId"), "bytes": byte_len}

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        print(f"[SQS] Error ({code}): {e}")
        # Helpful hint for the size error specifically
        if code == "InvalidParameterValue":
            print(f"[SQS] Hint: This usually means the body exceeded {SQS_LIMIT_BYTES} bytes.")
        if code == "AWS.SimpleQueueService.NonExistentQueue":
            print(f"[SQS] Queue '{queue_name}' does not exist. Is LocalStack up?")
        return {"ok": False, "error": code, "bytes": byte_len}

    except Exception as e:
        print(f"[SQS] Unexpected error: {e}")
        return {"ok": False, "error": "unexpected", "bytes": byte_len}

def receive_messages_from_sqs():
    """Receive messages from SQS queue"""
    
    print("Receiving messages from SQS queue...")
    
    sqs_client = boto3.client(
        'sqs',
        endpoint_url='http://localhost:4567',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    queue_name = 'test-queue'
    
    try:
        # Get queue URL
        queue_url_response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = queue_url_response['QueueUrl']
        
        # Receive messages
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1
        )
        
        if 'Messages' in response:
            print(f"📥 Received {len(response['Messages'])} messages:")
            for i, message in enumerate(response['Messages'], 1):
                print(f"   Message {i}:")
                print(f"     ID: {message['MessageId']}")
                print(f"     Body: {message['Body']}")
                print(f"     Receipt Handle: {message['ReceiptHandle'][:20]}...")
                print()
        else:
            print("📭 No messages available in queue")
            
    except ClientError as e:
        print(f"Error receiving messages: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)