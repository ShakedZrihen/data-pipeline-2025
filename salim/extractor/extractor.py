import logging
import os
import json
import boto3
from datetime import datetime, timezone, UTC
import urllib.parse
from normalizer import normalize
from processor import parse
from sqs_producer import send_to_sqs
import gzip



logger = logging.getLogger('Extractor')
REGION = 'us-east-1'
AWS_ACCESS_KEY = 'test'
AWS_SECRET_ACCESS_KEY = 'test'


s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)
sqs = boto3.client(
    'sqs',
    endpoint_url='http://localhost:4566',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)
dynamodb = boto3.resource(
    'dynamodb', 
    endpoint_url='http://localhost:4566',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)

# Add constants
BUCKET_NAME = "gov-price-files-hanif-2025"
DYNAMODB_TABLE = 'gov_price_data'
QUEUE_NAME = 'price-data-queue'
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

def lambda_handler(event, queue_url):
    table = dynamodb.Table(DYNAMODB_TABLE)
    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(record['s3']['object']['key'])
            
            response = s3.get_object(Bucket=bucket, Key=key)
            gz_content = response['Body'].read()
            print(f"File content: {key}")
            if key.endswith('.gz'):
                file_content = gzip.decompress(gz_content)
            else:
                print(f"File is not gzipped, processing raw content, file: {key}")
                continue
            
            print(f"Processing file: {key}, size: {len(file_content)} bytes")
            
            file_type = (key.split('/')[-1]).split('_')[0] 
            provider = key.split('/')[0].lower()  
            print(f"File type: {file_type}")
            parsed_data = parse(file_content, file_type=file_type)
            
            normalized_data = normalize(
                parsed_data, 
                provider=provider, 
                branch=key.split('/')[1],
                file_type=file_type
            )
            print(f"Normalized, Parsed {normalized_data[:5]} items from {key}")
            
            if not normalized_data:
                print(f"No valid data found for {key}")
                continue

            os.makedirs(OUTPUT_DIR, exist_ok=True)

            provider_dir = os.path.join(OUTPUT_DIR, provider)
            os.makedirs(provider_dir, exist_ok=True)

            filename = os.path.basename(key).replace('.gz', '.json')
            json_file_path = os.path.join(provider_dir, filename)

            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(normalized_data, f, ensure_ascii=False, indent=4)
            print(f"Wrote JSON to {json_file_path}")

            json_data = json.dumps(normalized_data, ensure_ascii=False)
            
            send_to_sqs(sqs, queue_url, key, json_data)
            # print(f"Sent data to SQS for file: {key}")
            
            try:
                file_timestamp = datetime.now(UTC).isoformat()
                table.put_item(
                    Item={
                        'source_key': key,
                        'timestamp': file_timestamp,
                        'data_count': str(len(normalized_data))
                    }
                )
                print(f"File data stored in DynamoDB")
            except Exception as dynamodb_error:
                print(f"Failed to store file data to DynamoDB: {dynamodb_error}")
                raise
            
            
    
        except Exception as e:
            print(f'Error: {str(e)}')
            
    return {
        'statusCode': 200,
        'body': json.dumps('Extraction completed successfully')
    }
        

def init_sqs(queue_name):
    try:
        response = sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                'DelaySeconds': '0',
                'MessageRetentionPeriod': '86400'
            }
        )
        return response['QueueUrl']
    except Exception as e:
        print(f"Failed to create SQS queue: {e}")
        raise

def init_dynamodb(table_name):
    try:
        print("Connecting to DynamoDB")
        existing_tables = dynamodb.meta.client.list_tables()['TableNames']
        
        if table_name not in existing_tables:
            print(f"Creating new DynamoDB table: {table_name}")
            table = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'source_key', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'source_key', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        else:
            table = dynamodb.Table(table_name)
            
        print(f"Connected to DynamoDB table: {table_name}")
        return table
        
    except Exception as e:
        print(f"Failed to connect to DynamoDB: {e}")
        raise

def list_bucket_files(bucket_name):
    try:
        print(f"Listing files in bucket: {bucket_name}")
        files = []
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                for obj in page['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
        
        print(f"Found {len(files)} files in bucket")
        return files
            
    except Exception as e:
        print(f"Failed to list bucket contents: {e}")
        raise

def scan_and_upload_files():
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        
        print(f"Scanning directories in: {parent_dir}")
        
        uploaded_files = []
        for dir_name in os.listdir(parent_dir):
            dir_path = os.path.join(parent_dir, dir_name)
            
            if not os.path.isdir(dir_path) or not dir_name == 'downloads':
                continue
                
            print(f"Scanning directory: {dir_name}")
            
            for root, _, files in os.walk(dir_path):
                for filename in files:
                    file_path = os.path.join(root, filename)

                    store_code = os.path.basename(os.path.dirname(file_path)).split('-')[-1] if os.path.basename(os.path.dirname(file_path)).split('-')[-1] is not None else "unknown_store"
                    store_name = os.path.basename(os.path.dirname(os.path.dirname(file_path)))

                    s3_key = f"{store_name}/{store_code}/{filename}"
                    try:
                        with open(file_path, 'rb') as file_obj:
                            s3.upload_fileobj(file_obj, BUCKET_NAME, s3_key)
                        
                        uploaded_files.append({
                            'store': store_name,
                            'store_code': store_code,
                            'filename': filename,
                            's3_key': s3_key
                        })
                        print(f"Uploaded: {s3_key}")
                    except Exception as upload_error:
                        print(f"Failed to upload {s3_key}: {upload_error}")

        print(f"Uploaded {len(uploaded_files)} files to S3")
        return uploaded_files
        
    except Exception as e:
        print(f"Failed to scan directories: {e}")
        raise

def main_test():
    try:
        table = init_dynamodb(DYNAMODB_TABLE)
        queue_url = init_sqs(QUEUE_NAME)
        
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
        except:
            s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"Created bucket: {BUCKET_NAME}")
        
        uploaded_files = scan_and_upload_files()
        
        s3_event = {
            "Records": []
        }
        
        for file in uploaded_files:
            s3_event["Records"].append({
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": REGION,
                "eventTime": datetime.now(timezone.utc).isoformat() + "Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": BUCKET_NAME},
                    "object": {"key": file['s3_key']}
                }
            })

        lambda_handler(s3_event, queue_url)
        
        files = list_bucket_files(BUCKET_NAME)
        
        print("Infrastructure initialized successfully")
        return {
            'table': table,
            'queue_url': queue_url,
            'files': files,
            'uploaded_files': uploaded_files
        }
    except Exception as e:
        print(f"Failed to initialize: {e}")
        raise
    



if __name__ == "__main__":
    resources = main_test()
    print(f"DynamoDB Table: {DYNAMODB_TABLE}")
    print(f"SQS Queue URL: {resources['queue_url']}")
    print(f"Total files in bucket: {len(resources['files'])}")
    print(f"Newly uploaded files: {len(resources['uploaded_files'])}")