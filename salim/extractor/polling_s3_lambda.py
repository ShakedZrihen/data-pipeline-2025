# simulate_s3_events.py
import os, sys
import json
import boto3
import time
from .lambda_.handler import lambda_handler

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, BASE)                 # כדי ש- 'salim' יזוהה
sys.path.insert(0, os.path.dirname(__file__))  # כדי ש- 'lambda_' יזוהה
HERE = os.path.abspath(os.path.dirname(__file__))                 # .../salim/extractor
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))    # .../data-pipeline-2025

if HERE not in sys.path:
    sys.path.insert(0, HERE)              
    
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
    
PROCESSED_KEYS_FILE = ".processed_keys.json"
POLL_INTERVAL_SECONDS = 20

os.environ.setdefault('S3_ENDPOINT', 'http://localhost:4566')
os.environ.setdefault('SQS_ENDPOINT', 'http://localhost:4566')
os.environ.setdefault('SQS_QUEUE_URL', 'http://localhost:4566/000000000000/my-queue')
os.environ.setdefault('S3_BUCKET', 'test-bucket')

"""
## polling_s3_lambda.py - Lambda Polling Runner

### How does it work?
- A script that runs in an infinite loop
- Every 20 seconds:
- Pulls the list of files from S3
- Skips files that have already been processed (according to `.processed_keys.json`)
- Calls lambda_handler on new files only
- Saves the keys sent so they don't send again

### How do you run it?
```bash
python salim/extractor/polling_s3_lambda.py
"""

def load_processed_keys():
    if os.path.exists(PROCESSED_KEYS_FILE):
        with open(PROCESSED_KEYS_FILE, 'r') as f:
            return set(json.load(f))
    return set()


def save_processed_keys(keys):
    with open(PROCESSED_KEYS_FILE, 'w') as f:
        json.dump(list(keys), f)


def poll_s3_and_trigger_lambda():
    bucket = os.environ['S3_BUCKET']
    processed_keys = load_processed_keys()

    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT'),
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

    print(f"Starting polling every {POLL_INTERVAL_SECONDS} seconds...")

    while True:
        try:
            response = s3.list_objects_v2(Bucket=bucket)

            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if not key.lower().endswith('.gz'):
                        print(f"⏭Skipping non-gz file: {key}")
                        continue
                    
                    if key in processed_keys:
                        print(f"Skipping already processed file: {key}")
                        continue

                    event = {
                        "Records": [
                            {
                                "eventName": "ObjectCreated:Put",
                                "s3": {
                                    "bucket": {"name": bucket},
                                    "object": {"key": key}
                                }
                            }
                        ]
                    }

                    print(f"\nSimulating lambda trigger for: {key}")
                    lambda_handler(event)
                    processed_keys.add(key)
                    save_processed_keys(processed_keys)
            else:
                print(f"ℹNo files found in bucket: {bucket}")
        except Exception as e:
            print(f"Error during polling: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


def poll_s3_and_trigger_lambda_once(prefix=None, limit=None):
    bucket = os.environ['S3_BUCKET']
    processed_keys = load_processed_keys()

    prefix = prefix or os.getenv('S3_PREFIX_FILTER', '').strip()   # למשל: "providers/ramilevi/"
    limit = int(limit if limit is not None else os.getenv('S3_LIMIT', '0'))  # 0 = ללא הגבלה

    print("Running single S3 scan (one pass)...")
    if prefix:
        print(f"Using prefix filter: {prefix}")
    if limit:
        print(f"Processing limit: {limit} files")

    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT'),
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

    processed = 0
    try:
        paginator = s3.get_paginator('list_objects_v2')
        kwargs = {'Bucket': bucket}
        if prefix:
            kwargs['Prefix'] = prefix

        for page in paginator.paginate(**kwargs):
            for obj in page.get('Contents', []):
                key = obj['Key']

                if not key.lower().endswith('.gz'):
                    print(f"⏭ Skipping non-gz file: {key}")
                    continue

                if key in processed_keys:
                    print(f"Skipping already processed file: {key}")
                    continue

                event = {
                    "Records": [{
                        "eventName": "ObjectCreated:Put",
                        "s3": {"bucket": {"name": bucket}, "object": {"key": key}}
                    }]
                }

                print(f"\nSimulating lambda trigger for: {key}")
                lambda_handler(event)
                processed_keys.add(key)
                processed += 1

                if limit and processed >= limit:
                    print(f"Reached limit ({limit}). Stopping.")
                    save_processed_keys(processed_keys)
                    return

        save_processed_keys(processed_keys)

    except Exception as e:
        print(f"Error during single-pass polling: {e}")

        
if __name__ == "__main__":
    # poll_s3_and_trigger_lambda()
    poll_s3_and_trigger_lambda_once(prefix="providers/ramilevi/", limit=5)
