import boto3
import json
import time
import os
from datetime import datetime
from botocore.config import Config

def create_s3_client():
    """Create S3 client for LocalStack"""
    return boto3.client(
        's3',
        endpoint_url='http://localstack:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )

def create_bucket_if_not_exists(s3_client, bucket_name):
    """Create S3 bucket if it doesn't exist"""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
    except:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket {bucket_name}")

def upload_price_file(s3_client, bucket_name, file_path):
    """Upload a price file to S3"""
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist")
        return False
    
    try:
        file_name = os.path.basename(file_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"prices/{timestamp}_{file_name}"
        
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        return True
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

def main():
    bucket_name = os.getenv('S3_BUCKET', 'price-data')
    source_file = os.getenv('SOURCE_FILE', '/data/prices.json')
    upload_interval = int(os.getenv('UPLOAD_INTERVAL', '30'))
    
    print("Starting S3 Producer...")
    print(f"Bucket: {bucket_name}")
    print(f"Source file: {source_file}")
    print(f"Upload interval: {upload_interval} seconds")
    
    # Wait for LocalStack to be ready
    time.sleep(10)
    
    s3_client = create_s3_client()
    create_bucket_if_not_exists(s3_client, bucket_name)
    
    while True:
        success = upload_price_file(s3_client, bucket_name, source_file)
        if success:
            print(f"File uploaded successfully at {datetime.now()}")
        else:
            print(f"Failed to upload file at {datetime.now()}")
        
        time.sleep(upload_interval)

if __name__ == "__main__":
    main()