#!/usr/bin/env python3
"""
S3 initialization script for LocalStack
Creates the test-bucket when the container starts
"""
import boto3
import time
import os
import json

def init_s3():
    """Initialize S3 bucket"""
    print("Initializing S3...")
    
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url='http://s3:4566',
                aws_access_key_id='test',
                aws_secret_access_key='test',
                region_name='us-east-1'
            )
            
            # Test connection
            s3_client.list_buckets()
            print("S3 connection established")
            break
            
        except Exception as e:
            print(f"Waiting for S3 to be ready... (attempt {retry_count + 1}/{max_retries})")
            retry_count += 1
            time.sleep(2)
    
    if retry_count >= max_retries:
        print("Failed to connect to S3 after maximum retries")
        return
    
    try:
        # Create test-bucket
        bucket_name = 'test-bucket'
        try:
            s3_client.create_bucket(
                Bucket=bucket_name
            )
            print(f"Created bucket: {bucket_name}")
        except Exception as e:
            if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                print(f"Bucket {bucket_name} already exists")
                return
            else:
                print(f"Error creating bucket: {e}")
                # Try without location constraint
                try:
                    s3_client.create_bucket(Bucket=bucket_name)
                    print(f"Created bucket: {bucket_name}")
                except Exception as e2:
                    print(f"Failed to create bucket even without location constraint: {e2}")
                    return
        
        # Set bucket policy to allow public read (for testing)
        bucket_policy = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Sid': 'PublicReadGetObject',
                    'Effect': 'Allow',
                    'Principal': '*',
                    'Action': 's3:GetObject',
                    'Resource': f'arn:aws:s3:::{bucket_name}/*'
                }
            ]
        }
        
        s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(bucket_policy)
        )
        print("Set bucket policy")
        
    except Exception as e:
        if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
            print(f"Bucket {bucket_name} already exists")
        else:
            print(f"Error creating bucket: {e}")

if __name__ == "__main__":
    init_s3()
