import boto3
import os
import sys
from botocore.exceptions import ClientError

class S3Manager:
    def __init__(self, bucket_name='supermarkets', endpoint_url='http://localhost:4566',
                 aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1'):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def upload_file(self, branch: str, file: str):
        """Upload a file to S3 bucket using LocalStack"""
        print("Uploading file to S3 bucket...")

        file_path = f'./supermarkets/{branch}/{file}'
        s3_key = f'{branch}/{file}'

        try:
            if not os.path.exists(file_path):
                print(f"Error: File '{file_path}' not found!")
                return False

            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            print(f"✅ {file_path} uploaded to s3://{self.bucket_name}/{s3_key}")
            self.list_files()
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                print(f"Error: Bucket '{self.bucket_name}' does not exist!")
                print("Make sure LocalStack services are running with: docker-compose up")
            else:
                print(f"Error uploading file: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False

    def list_files(self):
        """List all files in the S3 bucket"""
        print("\nFiles in bucket:")
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' in response:
                for obj in response['Contents']:
                    filename = obj['Key']
                    size = obj['Size']
                    modified = obj['LastModified']
                    print(f"  - {filename} (Size: {size} bytes, Modified: {modified})")
            else:
                print("  No files found in bucket")
        except ClientError as e:
            print(f"Error listing files: {e}")

    def clear_bucket(self):
        """Clear all files from S3 bucket using LocalStack"""
        print("Clearing all files from S3 bucket...")

        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' not in response:
                print("✅ Bucket is already empty")
                return True

            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            if objects_to_delete:
                delete_response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
                deleted_count = len(delete_response.get('Deleted', []))
                print(f"✅ Successfully deleted {deleted_count} files from s3://{self.bucket_name}")
                for deleted in delete_response.get('Deleted', []):
                    print(f"  - Deleted: {deleted['Key']}")
                if 'Errors' in delete_response:
                    print("❌ Some files failed to delete:")
                    for error in delete_response['Errors']:
                        print(f"  - {error['Key']}: {error['Message']}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                print(f"Error: Bucket '{self.bucket_name}' does not exist!")
                print("Make sure LocalStack services are running with: docker-compose up")
            else:
                print(f"Error clearing bucket: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False

if __name__ == "__main__":
    s3 = S3Manager()
    # Example usage:
    # s3.upload_file('branch_name', 'file_name.csv')
    s3.clear_bucket()