import boto3
import os
from botocore.exceptions import ClientError

ENDPOINT_URL = "http://localhost:4566"
REGION_NAME = "us-east-1"
BUCKET_NAME = "providers"
QUEUE_NAME = "prices-queue"
TABLE_NAME = "LastRunTimestamps"

def setup_infrastructure(s3, sqs, dynamodb):
    print("--- 1. Setting up infrastructure ---")
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' created.")
    except ClientError as e:
        if e.response['Error']['Code'] in ['BucketAlreadyOwnedByYou', 'BucketAlreadyExists']:
            print(f"ℹ️  Bucket '{BUCKET_NAME}' already exists.")
        else:
            raise e
    try:
        sqs.create_queue(QueueName=QUEUE_NAME)
        print(f"✅ SQS Queue '{QUEUE_NAME}' created.")
    except ClientError:
        print(f"ℹ️  SQS Queue '{QUEUE_NAME}' already exists.")
        pass
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
        else:
            raise e
    print("--- Infrastructure setup complete ---\n")

def upload_files_to_s3(s3):
    print("--- 2. Uploading files to S3 ---")
    providersName = ["keshet", "ramilevi", "yohananof"]
    folderPath = 'Crawler/out/'
    for provider in providersName:
        print(f"Running on Provider: {provider}")
        provider_full_path = os.path.join(folderPath, provider)
        if not os.path.exists(provider_full_path):
            print(f"  - Directory not found, skipping: {provider_full_path}")
            continue
        for branch_folder in os.listdir(provider_full_path):
            branch_path = os.path.join(provider_full_path, branch_folder)
            if os.path.isdir(branch_path):
                print(f"  - Running through Branch: {branch_folder}")
                for file_name in os.listdir(branch_path):
                    local_file_path = os.path.join(branch_path, file_name)
                    s3_key = f"{provider}/{branch_folder}/{file_name}"
                    s3.upload_file(local_file_path, BUCKET_NAME, s3_key)
                    print(f"    ✅ Uploaded {file_name}")
    print("--- File upload complete ---\n")

def main():
    s3_client = boto3.client('s3', endpoint_url=ENDPOINT_URL, region_name=REGION_NAME)
    sqs_client = boto3.client('sqs', endpoint_url=ENDPOINT_URL, region_name=REGION_NAME)
    dynamodb_client = boto3.client('dynamodb', endpoint_url=ENDPOINT_URL, region_name=REGION_NAME)
    setup_infrastructure(s3_client, sqs_client, dynamodb_client)
    upload_files_to_s3(s3_client)

if __name__ == "__main__":
    main()