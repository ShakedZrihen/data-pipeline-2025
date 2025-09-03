#!/usr/bin/env python3
"""
Create S3 bucket for the crawler
"""
import boto3
import os

def create_bucket():
    """Create S3 bucket if it doesn't exist"""
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('S3_ENDPOINT', 'http://localhost:4566'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        )
        
        bucket_name = 'test-bucket'
        
        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"✅ Bucket {bucket_name} already exists")
            return
        except:
            pass
        
        # Create bucket
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✅ Created bucket: {bucket_name}")
        
    except Exception as e:
        print(f"❌ Error creating bucket: {e}")

if __name__ == "__main__":
    create_bucket()
