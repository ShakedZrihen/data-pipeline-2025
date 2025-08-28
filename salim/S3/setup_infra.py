import boto3
import os
import json
from botocore.exceptions import ClientError

ENDPOINT_URL = "http://localhost:4566"
REGION_NAME = "us-east-1"
BUCKET_NAME = "providers"
QUEUE_NAME = "prices-queue"
TABLE_NAME = "LastRunTimestamps"

def main():
    print("--- Setting up infrastructure ---")
    s3 = boto3.client('s3', endpoint_url=ENDPOINT_URL, region_name=REGION_NAME)
    sqs = boto3.client('sqs', endpoint_url=ENDPOINT_URL, region_name=REGION_NAME)
    dynamodb = boto3.client('dynamodb', endpoint_url=ENDPOINT_URL, region_name=REGION_NAME)

    # 1. Setup S3 Bucket
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' created.")
    except ClientError as e:
        if e.response['Error']['Code'] in ['BucketAlreadyOwnedByYou', 'BucketAlreadyExists']:
            print(f"ℹ️  Bucket '{BUCKET_NAME}' already exists.")
        else: raise e

    # 2. Setup SQS with Dead-Letter Queue (DLQ)
    dlq_name = QUEUE_NAME + "-dlq"
    try:
        # Create the Dead-Letter Queue first
        print(f"Attempting to create SQS Dead-Letter Queue '{dlq_name}'...")
        dlq_response = sqs.create_queue(QueueName=dlq_name)
        dlq_url = dlq_response['QueueUrl']
        
        # Get the ARN of the DLQ, which is needed for the policy
        dlq_attributes = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=['QueueArn'])
        dlq_arn = dlq_attributes['Attributes']['QueueArn']
        print(f"✅ DLQ '{dlq_name}' is ready.")

        # Create the main queue and link it to the DLQ
        print(f"Attempting to create main SQS Queue '{QUEUE_NAME}'...")
        redrive_policy = {
            'deadLetterTargetArn': dlq_arn,
            'maxReceiveCount': '3'  # After 3 failed attempts, move to DLQ
        }
        
        sqs.create_queue(
            QueueName=QUEUE_NAME,
            Attributes={
                'RedrivePolicy': json.dumps(redrive_policy)
            }
        )
        print(f"✅ Main queue '{QUEUE_NAME}' is ready and linked to DLQ.")

    except ClientError as e:
        print(f"⚠️  Could not create or link queues. This might be okay if they already exist. Error: {e}")
        pass

    # 3. Setup DynamoDB Table
    try:
        dynamodb.create_table(
            TableName=TABLE_NAME,
            AttributeDefinitions=[{'AttributeName': 'ProviderBranchType', 'AttributeType': 'S'}],
            KeySchema=[{'AttributeName': 'ProviderBranchType', 'KeyType': 'HASH'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print(f"✅ DynamoDB Table '{TABLE_NAME}' created.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"ℹ️  DynamoDB Table '{TABLE_NAME}' already exists.")
        else: raise e

    print("--- Infrastructure setup complete ---\n")

if __name__ == "__main__":
    main()