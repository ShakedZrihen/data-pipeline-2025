#!/usr/bin/env python3

import boto3
import sys
from botocore.exceptions import ClientError

def clear_s3_bucket():
    """Clear all files from S3 bucket using LocalStack"""
    
    print("Clearing all files from S3 bucket...")
    
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    bucket_name = 'test-bucket'
    
    try:
        # List all objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' not in response:
            print("✅ Bucket is already empty")
            return
        
        # Delete all objects
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        
        if objects_to_delete:
            delete_response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete}
            )
            
            deleted_count = len(delete_response.get('Deleted', []))
            print(f"✅ Successfully deleted {deleted_count} files from s3://{bucket_name}")
            
            # List deleted files
            for deleted in delete_response.get('Deleted', []):
                print(f"  - Deleted: {deleted['Key']}")
        
        # Check for any deletion errors
        if 'Errors' in delete_response:
            print("❌ Some files failed to delete:")
            for error in delete_response['Errors']:
                print(f"  - {error['Key']}: {error['Message']}")
                
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            print(f"Error: Bucket '{bucket_name}' does not exist!")
            print("Make sure LocalStack services are running with: docker-compose up")
        else:
            print(f"Error clearing bucket: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    clear_s3_bucket()