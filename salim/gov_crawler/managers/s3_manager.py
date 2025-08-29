import boto3
import os
import sys
from botocore.exceptions import ClientError
from pathlib import Path, PurePosixPath
from utils.enums import ENUMS

class S3Manager:
    def __init__(self, bucket_name=ENUMS.BUCKET_NAME.value, endpoint_url=ENUMS.S3_ENDPOINT_URL.value,
                 aws_access_key_id=ENUMS.AWS_ACCESS_KEY_ID.value, aws_secret_access_key=ENUMS.AWS_SECRET_ACCESS_KEY.value, region_name=ENUMS.AWS_REGION.value):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
    def upload_file_from_path(self, local_path: str, s3_key: str):
        s3_key = str(PurePosixPath(s3_key)) 
        p = Path(local_path)
        if not p.exists():
            print(f"Error: File '{p}' not found!")
            return False
        try:
            self.s3_client.upload_file(str(p), self.bucket_name, s3_key)
            print(f"{p} uploaded to s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            print(f"Error uploading to S3: {e}")
            return False


    def clear_bucket(self):
        """Clear all files from S3 bucket using LocalStack"""
        print("Clearing all files from S3 bucket...")

        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' not in response:
                print("Bucket is already empty")
                return True

            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            if objects_to_delete:
                delete_response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
                deleted_count = len(delete_response.get('Deleted', []))
                print(f"Successfully deleted {deleted_count} files from s3://{self.bucket_name}")
                for deleted in delete_response.get('Deleted', []):
                    print(f"  - Deleted: {deleted['Key']}")
                if 'Errors' in delete_response:
                    print("Some files failed to delete:")
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
    s3.clear_bucket()