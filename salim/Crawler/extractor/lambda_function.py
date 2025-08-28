import json
import gzip
import os
import math

from utils import client, parse_s3_key
from file_parser import process_file_content
from sqs_producer import send as send_to_sqs
from ddb_state import upsert_last_run

QUEUE_URL = os.getenv("QUEUE_URL")
DDB_TABLE = os.getenv("DDB_TABLE")

s3 = client("s3")
sqs = client("sqs")
ddb = client("dynamodb")

def create_chunks(data, chunk_size):
    """Splits a list into smaller chunks of a specified size."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def lambda_handler(event, context):
    print("--- FULL LAMBDA TRIGGERED! ---")

    for rec in event.get("Records", []):
        key = None
        try:
            bucket = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]

            print(f"--> Processing file: {key}")

            provider, branch, file_type, timestamp = parse_s3_key(key)

            obj = s3.get_object(Bucket=bucket, Key=key)
            gz_bytes = obj["Body"].read()
            xml_text = gzip.decompress(gz_bytes).decode("utf-8")

            result = process_file_content(key, xml_text)
            items = result.get("items", [])

            if not items:
                print(f"--> No valid items found in {key}. Skipping.")
                continue

            # --- זה התיקון ---
            # נחלק את רשימת המוצרים הגדולה לחלקים של 100
            chunk_size = 100 
            item_chunks = list(create_chunks(items, chunk_size))
            total_chunks = len(item_chunks)
            print(f"--> Splitting {len(items)} items into {total_chunks} chunks of up to {chunk_size} items each.")

            for i, chunk in enumerate(item_chunks):
                # ניצור הודעה חדשה עבור כל חלק
                message_payload = {
                    "provider": result["provider"],
                    "branch": result["branch"],
                    "type": result["type"],
                    "timestamp": result["timestamp"],
                    "items": chunk # החלק הנוכחי של המוצרים
                }
                send_to_sqs(sqs, QUEUE_URL, message_payload)
                print(f"--> Sent chunk {i+1}/{total_chunks} for {key} to SQS.")

            upsert_last_run(ddb, DDB_TABLE, provider, branch, file_type, timestamp)
            print(f"--> Updated DynamoDB state for {key}.")

        except Exception as e:
            print(f"[ERROR] Failed to process record with key: '{key}'. Reason: {e}")
            continue

    return {"statusCode": 200, "body": json.dumps("OK")}