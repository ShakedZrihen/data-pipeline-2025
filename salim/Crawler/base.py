from abc import ABC, abstractmethod
import boto3
from botocore.exceptions import ClientError
import os
import sys
import requests
import time
from utils import (
    convert_xml_to_json,
    download_file_from_link,
    extract_and_delete_gz,
)

class CrawlerBase(ABC):

    
    @abstractmethod
    def crawl(self)->list[str]:
        pass

    def save_file(self, link):
        file = download_file_from_link(link, ".")
        file_name = file.split("/")[-1]
        file_path = f"./{file_name}"
        extract_and_delete_gz(file_path)
        return file_path

    def upload_file_to_s3(self, file_path, s3_key):
        """
        Upload a file to S3 bucket.
        
        Args:
            file_path (str): Path to the file to upload
            s3_key (str): S3 key (path) where to store the file
        """
        print(f"Uploading {file_path} to S3 bucket...")
        
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:4566',
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        
        bucket_name = 'test-bucket'
        
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                print(f"Error: File '{file_path}' not found!")
                return False
            
            # Create timestamped directory key
            timestamp = int(time.time())
            dir_key = f"{s3_key}/{timestamp}/"
            
            # Create directory in S3
            try:
                s3_client.put_object(Bucket=bucket_name, Key=dir_key)
                print(f"Created directory '{dir_key}' in bucket '{bucket_name}'")
            except Exception as e:
                print(f"Error creating directory '{dir_key}': {e}")
            
            # Upload the file
            file_name = os.path.basename(file_path)
            s3_file_key = f"{dir_key}{file_name}"
            
            s3_client.upload_file(file_path, bucket_name, s3_file_key)
            print(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_file_key}")
            
            # List files in bucket
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
            
            return True
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                print(f"Error: Bucket '{bucket_name}' does not exist!")
                print("Make sure LocalStack services are running with: docker-compose up")
            else:
                print(f"Error uploading file: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False

