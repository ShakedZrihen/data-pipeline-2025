# simulate_s3_events.py
import os, sys
import json
import boto3
import time
HERE = os.path.abspath(os.path.dirname(__file__))                 # .../salim/extractor
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))    # .../data-pipeline-2025

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from salim.extractor.lambda_.handler import lambda_handler

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

# helper function if we want to run it for small number of files
def poll_s3_and_trigger_lambda_limited(prices_limit=2, promos_limit=0):
    bucket = os.environ['S3_BUCKET']
    processed_keys = load_processed_keys()

    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT'),
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )

    print(f"Scanning bucket once, processing up to {prices_limit} Prices and {promos_limit} Promos files...")

    prices_processed = 0
    promos_processed = 0

    try:
        response = s3.list_objects_v2(Bucket=bucket)

        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']

                if not key.lower().endswith('.gz'):
                    print(f"⏭ Skipping non-gz file: {key}")
                    continue

                if key in processed_keys:
                    print(f"Skipping already processed file: {key}")
                    continue

                is_prices = "prices" in key.lower()
                is_promos = "promos" in key.lower()

                if is_prices and prices_processed >= prices_limit:
                    continue
                if is_promos and promos_processed >= promos_limit:
                    continue
                if not is_prices and not is_promos:
                    print(f"Unknown file type: {key}")
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

                print(f"\n🎯 Simulating lambda trigger for: {key}")
                lambda_handler(event)

                processed_keys.add(key)
                save_processed_keys(processed_keys)

                if is_prices:
                    prices_processed += 1
                elif is_promos:
                    promos_processed += 1

                if prices_processed >= prices_limit and promos_processed >= promos_limit:
                    print("Reached limits for both file types. Stopping.")
                    break

        else:
            print(f" No files found in bucket: {bucket}")

    except Exception as e:
        print(f"Error during polling: {e}")


if __name__ == "__main__":
    poll_s3_and_trigger_lambda()
    # poll_s3_and_trigger_lambda_limited(prices_limit=2, promos_limit=2)
