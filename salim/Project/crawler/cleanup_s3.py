#!/usr/bin/env python3
"""
Clear S3 bucket on container startup
"""
import boto3
import os

def clear_s3_bucket():
    """Clear all files from S3 bucket"""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=os.environ.get('S3_ENDPOINT', 'http://s3:4566'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID', 'test'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'test'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        
        bucket_name = 'test-bucket'
        
        # List all objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            if objects_to_delete:
                print(f"Clearing {len(objects_to_delete)} files from S3 bucket...")
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
                print("S3 bucket cleared successfully")
            else:
                print("S3 bucket is already empty")
        else:
            print("S3 bucket is empty")
            
    except Exception as e:
        print(f"Warning: Could not clear S3 bucket: {e}")

if __name__ == "__main__":
    clear_s3_bucket()
