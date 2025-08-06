import boto3
import os

def upload_all_files_to_localstack(bucket_name="test-bucket"):
    s3 = boto3.client(
        's3',
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    )

    data_folder = "../../assignments/assignment_2/data"

    for root, dirs, files in os.walk(data_folder):
        for file in files:
            local_path = os.path.join(root, file)

            relative_path = os.path.relpath(local_path, start=data_folder)

            s3_key = relative_path.replace(os.sep, "/")

            print(f"Uploading {local_path} → s3://{bucket_name}/{s3_key}")
            s3.upload_file(local_path, bucket_name, s3_key)

    print("All files successfully uploaded to S3!")

if __name__ == "__main__":
    upload_all_files_to_localstack()