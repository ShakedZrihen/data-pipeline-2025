import boto3
from botocore.exceptions import ClientError

def create_bucket():
    """Create the test-bucket in LocalStack"""
    
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    bucket_name = 'test-bucket'
    
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' created successfully!")
        
        # List buckets to confirm
        response = s3_client.list_buckets()
        print("\nAvailable buckets:")
        for bucket in response['Buckets']:
            print(f"  - {bucket['Name']}")
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyExists':
            print(f"✅ Bucket '{bucket_name}' already exists!")
        else:
            print(f"❌ Error creating bucket: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    create_bucket() 