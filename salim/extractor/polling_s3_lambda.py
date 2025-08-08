# simulate_s3_events.py
import os
import json
import boto3
import time
from lambda_.handler import lambda_handler

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


if __name__ == "__main__":
    poll_s3_and_trigger_lambda()
