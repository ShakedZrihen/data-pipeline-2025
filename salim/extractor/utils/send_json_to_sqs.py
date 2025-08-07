import boto3
import os
import json

def send_json_to_sqs(json_data):
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
