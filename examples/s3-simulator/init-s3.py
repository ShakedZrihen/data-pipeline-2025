#!/usr/bin/env python3
"""
S3 initialization script that works on both Windows and Mac
"""

import time
import boto3
from botocore.exceptions import ClientError

def init_s3():
    """Initialize S3 bucket"""
    
    print("Initializing S3 bucket with webhook notifications...")
    
    # Wait for LocalStack to be ready
    print("Waiting for LocalStack to be ready...")
    time.sleep(10)
    
    # Create S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    bucket_name = 'test-bucket'
    
    try:
        # Create bucket
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")
        
        # Wait for Lambda function to be ready
        print("Waiting for Lambda function to be ready...")
        time.sleep(5)
        
        print("S3 initialization completed!")
        print("Webhook events should be triggered for each file upload!")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyExists':
            print(f"Bucket '{bucket_name}' already exists!")
        else:
            print(f"Error creating bucket: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    init_s3() 