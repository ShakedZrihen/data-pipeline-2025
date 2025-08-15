import os
import json
import gzip

from utils import client, parse_s3_key
from file_parser import process_file_content
from sqs_producer import send as send_to_sqs
from ddb_state import upsert_last_run

QUEUE_URL = os.getenv("QUEUE_URL", "http://localstack:4566/000000000000/prices-queue")
DDB_TABLE = os.getenv("DDB_TABLE", "LastRunTimestamps")

s3 = client("s3")
sqs = client("sqs")
ddb = client("dynamodb")

def _handle_single_record(record):
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    provider, branch, file_type, timestamp = parse_s3_key(key)

    obj = s3.get_object(Bucket=bucket, Key=key)
    gz_bytes = obj["Body"].read()
    xml_text = gzip.decompress(gz_bytes).decode("utf-8")

    normalized_key = f"{provider}/{branch}/{file_type}_{timestamp}.gz"
    result = process_file_content(normalized_key, xml_text)
    items = result.get("items", result)

    out = {
        "provider": provider,
        "branch": branch,
        "type": file_type,
        "timestamp": timestamp,
        "items": items,
    }

    send_to_sqs(sqs, QUEUE_URL, out)

    upsert_last_run(ddb, DDB_TABLE, provider, branch, file_type, timestamp)

    safe = f"{provider}_{branch}_{file_type}_{timestamp}".replace("/", "_").replace(" ", "_")
    with open(f"/tmp/{safe}.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

def lambda_handler(event, context):
    for rec in event.get("Records", []):
        try:
            _handle_single_record(rec)
        except Exception as e:
            print(f"[ERROR] {e}")
            continue
    return {"statusCode": 200, "body": json.dumps("OK")}
