import boto3
import os
import sys
from botocore.exceptions import ClientError

def upload_file_to_s3():
    """Upload ShakedZrihen.txt to S3 bucket using LocalStack"""
    
    print("Uploading shaked.txt to S3 bucket...")
    
    s3_client = boto3.client(
        's3',
        endpoint_url='https://b659bdb2de99.ngrok-free.app',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    bucket_name = 'test-bucket'
    file_path = './ShakedZrihen.txt'
    s3_key = 'ShakedZrihen2.txt'
    
    try:
        if not os.path.exists(file_path):
            print(f"Error: File '{file_path}' not found!")
            sys.exit(1)
        
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"✅ {file_path} uploaded to s3://{bucket_name}/{s3_key}")        
        print("\nFiles in bucket:")
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                filename = obj['Key']
                size = obj['Size']
                modified = obj['LastModified']
                print(f"  - {filename} (Size: {size} bytes, Modified: {modified})")
        else:
            print("  No files found in bucket")
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print(f"Error: Bucket '{bucket_name}' does not exist!")
            print("Make sure LocalStack services are running with: docker-compose up")
        else:
            print(f"Error uploading file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    upload_file_to_s3()