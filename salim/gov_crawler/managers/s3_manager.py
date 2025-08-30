import boto3
import os
import sys
from botocore.exceptions import ClientError
from pathlib import Path, PurePosixPath

class S3Manager:
    def __init__(
            self,
            bucket_name: str | None = None,
            endpoint_url: str | None = None,
            aws_access_key_id: str | None = None,
            aws_secret_access_key: str | None = None,
            region_name: str | None = None,
        ):
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET")
        self.endpoint_url = endpoint_url or os.getenv("S3_ENDPOINT_URL")
        self.aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region_name = region_name or os.getenv("AWS_REGION")

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
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
    s3 = S3Manager(
        bucket_name=os.getenv("S3_BUCKET"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    s3.clear_bucket()