# utils.py
import boto3
from botocore.exceptions import ClientError

def iter_sqs_batches(
    queue_name: str = "test-queue",
    endpoint_url: str = "http://localhost:4567",
    region_name: str = "us-east-1",
    aws_access_key_id: str = "test",
    aws_secret_access_key: str = "test",
    wait_time_seconds: int = 5,        # long-poll each request
    max_number_per_poll: int = 10,     # SQS cap is 10
    max_empty_polls: int = 1
):
    """
    Yield lists of messages (<= max_number_per_poll) from SQS.
    Stops after `max_empty_polls` consecutive empty receives.
    """
    sqs_client = boto3.client(
        "sqs",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )
    queue_url = sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]

    empty_polls = 0
    while True:
        try:
            resp = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_number_per_poll,
                WaitTimeSeconds=wait_time_seconds,
                MessageAttributeNames=["All"],
                AttributeNames=["All"],
            )
        except ClientError as e:
            print(f"[SQS] receive error: {e}")
            break

        batch = resp.get("Messages", []) or []
        if not batch:
            empty_polls += 1
            if empty_polls >= max_empty_polls:
                break
            continue

        yield batch
