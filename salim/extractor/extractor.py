import boto3
import os

from utils import extract_and_delete_gz, convert_xml_to_json, sanitize_path_component

def extract_from_s3():
    try:
        print("starting boto3")
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv('S3_ENDPOINT', 'http://localhost:4566'),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'test'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'test'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        )
        bucket_name = os.getenv('S3_BUCKET', 'providers')
        
        work_root = os.getenv("WORK_DIR", "work")
        os.makedirs(work_root, exist_ok=True)

        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                filename = os.path.basename(key)
                print(f"↓ Downloading {key} → ./{filename}")

                parts = [sanitize_path_component(p) for p in key.split("/")]
                local_path = os.path.join(work_root, *parts)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                s3_client.download_file(bucket_name, key, local_path)
                print(f"→ {local_path}")
        
                extracted = extract_and_delete_gz(local_path, delete_gz=True)
                if extracted:
                    convert_xml_to_json(extracted)
        else:
            print("No objects found in the bucket.")
   
    except Exception as e:
            print(f"Error handling GET request: {e}")

if __name__ == "__main__":
    extract_from_s3()