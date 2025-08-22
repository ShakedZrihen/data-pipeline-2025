
import boto3
import os
import sys
from botocore.exceptions import ClientError

def upload_file_to_s3(provider, branch, file_path):
    """
    Upload a .gz file to S3 under:
    s3://providers-bucket/<provider>/<branch>/pricesFull_xxx or promoFull_xxx
    """

    print(f"Uploading {file_path} to S3 bucket under '{provider}/{branch}/'...")

    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

    bucket_name = 'providers-bucket'
    file_name = os.path.basename(file_path)

    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found!")
        sys.exit(1)

    # Build S3 key based on file type
    if "pricesFull" in file_name:
        s3_key = f"providers/{provider}/{branch}/pricesFull_{file_name.split('pricesFull_')[-1]}"
    elif "promoFull" in file_name:
        s3_key = f"providers/{provider}/{branch}/promoFull_{file_name.split('promoFull_')[-1]}"
    else:
        print(f"Invalid file name — must contain 'pricesFull' or 'promoFull': {file_name}")
        sys.exit(1)


    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded to s3://{bucket_name}/{s3_key}")

        # Optional: List current objects
        print("\nFiles in bucket:")
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"  - {obj['Key']} (Size: {obj['Size']} bytes, Modified: {obj['LastModified']})")
        else:
            print("  No files found in bucket")

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"Bucket '{bucket_name}' does not exist. Start LocalStack or create the bucket.")
        else:
            print(f"Upload failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

# Optional CLI usage
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python upload_test.py <provider> <branch> <file_path>")
        sys.exit(1)

    provider = sys.argv[1]
    branch = sys.argv[2]
    file_path = sys.argv[3]

    upload_file_to_s3(provider, branch, file_path)
