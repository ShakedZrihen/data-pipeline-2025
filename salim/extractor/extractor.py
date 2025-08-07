import boto3
import os

def extract_from_s3():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('S3_ENDPOINT', 'http://localhost:4566'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        )
        bucket_name = os.getenv('S3_BUCKET', 'providers')

        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"Found object: {obj['Key']} (Size: {obj['Size']} bytes)")
        else:
            print("No objects found in the bucket.")
   
    except Exception as e:
            print(f"Error handling GET request: {e}")

