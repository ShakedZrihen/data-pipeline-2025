import boto3
import sys
from botocore.exceptions import ClientError

def receive_messages_from_sqs(
    queue_name: str = "test-queue",
    endpoint_url: str = "http://localhost:4567",
    region_name: str = "us-east-1",
    aws_access_key_id: str = "test",
    aws_secret_access_key: str = "test",
    wait_time_seconds: int = 5,        # long-poll each request
    max_number_per_poll: int = 10,     # SQS max per call
    max_empty_polls: int = 1,          # stop after these consecutive empties
    delete_after_read: bool = False    # set True if you want to drain the queue
):
    """
    Receive *all* currently available messages from an SQS queue.
    Returns a list of message dicts (each has MessageId, Body, ReceiptHandle, ...).
    """
    try:
        sqs_client = boto3.client(
            "sqs",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

        queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]

        all_messages = []
        empty_polls = 0

        while True:
            resp = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_number_per_poll,
                WaitTimeSeconds=wait_time_seconds,
                MessageAttributeNames=["All"],
                AttributeNames=["All"],
            )

            batch = resp.get("Messages", [])
            if not batch:
                empty_polls += 1
                if empty_polls >= max_empty_polls:
                    break
                continue

            all_messages.extend(batch)

            # Optionally delete as you read so they don't reappear after visibility timeout
            # if delete_after_read:
            #     entries = [{"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]} for m in batch]
            #     sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)

            # keep polling until we hit an empty batch

        return all_messages

    except ClientError as e:
        print(f"Error receiving messages: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
