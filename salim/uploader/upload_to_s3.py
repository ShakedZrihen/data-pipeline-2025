import boto3
import os
from botocore.exceptions import ClientError

s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

bucket_name = "my-bucket"

# Create the bucket if it doesn't exist
try:
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created.")
except Exception:
    print(f"Bucket '{bucket_name}' may already exist. Skipping creation.")

def upload_file_to_s3(local_path, s3_key):
    """
    Upload a local file to S3 (LocalStack) and print the updated file list.

    Parameters:
        local_path (str): Path to the local file
        s3_key (str): Desired path/key in the S3 bucket
    """
    if not os.path.exists(local_path):
        print(f"❌ Error: File '{local_path}' not found!")
        return

    try:
        print(f"📤 Uploading {local_path} to s3://{bucket_name}/{s3_key}")
        s3_client.upload_file(local_path, bucket_name, s3_key)
        print("✅ Upload successful!")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "NoSuchBucket":
            print(f"❌ Error: Bucket '{bucket_name}' does not exist!")
            print("💡 Make sure LocalStack is running using: docker-compose up")
        else:
            print(f"❌ S3 Upload error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")