from decimal import Decimal
import os
import json
from datetime import datetime, time, timezone, UTC
import traceback
import urllib.parse
from processor import parse
from sqs_producer import send_to_sqs
import gzip
import time
from settings import (
    BUCKET_NAME, QUEUE_NAME, INTERVAL_MIN,
    DYNAMODB_TABLE, OUTPUT_DIR, s3, dynamodb,sqs
)



def get_all_s3_keys(bucket_name: str, prefix: str | None = None) -> list[str]:
    
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    params = {"Bucket": bucket_name}
    if prefix:
        params["Prefix"] = prefix

    for page in paginator.paginate(**params):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

def lambda_handler():
    init_dynamodb(DYNAMODB_TABLE)
    table = dynamodb.Table(DYNAMODB_TABLE)
    queue_url = init_sqs(QUEUE_NAME)
        
    files = get_all_s3_keys(BUCKET_NAME)
    print(f"Found {files} files in bucket {BUCKET_NAME}")

    for key in files:
        try:
            bucket = BUCKET_NAME
            key = urllib.parse.unquote_plus(key)
            print(f"Processing file: {key} from bucket: {bucket}")
            
            response = s3.get_object(Bucket=bucket, Key=key)
            gz_content = response['Body'].read()
            if key.endswith('.gz'):
                print(f"Decompressing gzipped file: {type(gz_content)}")
                file_content = gzip.decompress(gz_content)
            else:
                print(f"File is not gzipped, processing raw content, file: {key}")
                continue
            
            print(f"Processing file: {key}, size: {len(file_content)} bytes")

            parts = key.split('_')
            provider = parts[0].lower()
            branch = parts[1].lower()
            file_type = parts[2]
            timestamp = parts[3].replace('.gz', '')
            
            print(f"Extracted metadata - Provider: {provider}, Branch: {branch}, File Type: {file_type}")

            parsed_data = parse(file_content, file_type=file_type, provider=provider, branch=branch)

            print(f"Parsed {len(parsed_data.get('items', []))} items from {key}. data: {parsed_data.get('items', [])[:3]}...")
            
            if not parsed_data.get('items'):
                print(f"No valid data found for {key}")
                continue

            os.makedirs(OUTPUT_DIR, exist_ok=True)

            filename = os.path.basename(key).replace('.gz', '.json')
            json_file_path = os.path.join(OUTPUT_DIR, filename)

            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(parsed_data.get('items', []), f, ensure_ascii=False, indent=4)
            print(f"Wrote JSON to {json_file_path}")

            json_data = json.dumps(parsed_data.get('items', []), ensure_ascii=False)

            send_to_sqs(sqs, queue_url, key, json_data)
            
            try:
                file_timestamp = datetime.fromtimestamp(int(timestamp), timezone.utc).isoformat()
                dedupe_key = f"{provider}_{branch}_{file_type}"
                table.update_item(
                Key={'source_key': dedupe_key},
                UpdateExpression=(
                    "SET provider = :p, "
                    "    branch = :b, "
                    "    file_type = :ft, "
                    "    last_updated_at = :t, "
                    "    data_count = :c"
                ),
                ExpressionAttributeValues={
                    ':p': provider,
                    ':b': branch,
                    ':ft': file_type,
                    ':t': file_timestamp,
                    ':c': Decimal(len(parsed_data.get('items', []))),  # store as DynamoDB Number
                },
                ReturnValues="NONE"
                )
                print(f"File data stored in DynamoDB")
                
            except Exception as dynamodb_error:
                print(f"Failed to store file data to DynamoDB: {dynamodb_error}")
                raise
            
            s3.delete_object(Bucket=bucket, Key=key)
            print(f"Deleted S3 object: {key}")
            
        except Exception as e:
            print(f'Error: {str(e)}, key: {key}')

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

                    try:
                        with open(file_path, 'rb') as file_obj:
                            s3.upload_fileobj(file_obj, BUCKET_NAME, filename)

                        uploaded_files.append({
                            's3_key': filename,
                        })
                        print(f"Uploaded: {filename}")
                    except Exception as upload_error:
                        print(f"Failed to upload {filename}: {upload_error}")

        print(f"Uploaded {len(uploaded_files)} files to S3")
        return uploaded_files
        
    except Exception as e:
        print(f"Failed to scan directories: {e}")
        raise

if __name__ == "__main__":
    while True:
            start = time.time()
            try:
                lambda_handler()
            except Exception as e:
                print(f"[extractor] uncaught error: {e}")
                traceback.print_exc()
            elapsed = time.time() - start
            to_sleep = max(0, INTERVAL_MIN * 60 - elapsed)
            print(f"[extractor] sleeping {int(to_sleep)}s")
            time.sleep(to_sleep)