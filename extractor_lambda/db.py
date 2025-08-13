import os
import time
import boto3
from botocore.config import Config

def _client(service):
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    region = os.getenv("AWS_REGION", "us-east-1")
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client(service, region_name=region, endpoint_url=endpoint, config=cfg)

def upsert_last_run(provider, branch, type_, iso_ts):
    table_name = os.getenv("LAST_RUN_TABLE", "price-extractor-last-run")
    ddb = _client("dynamodb")
    pk = f"{provider}#{branch}#{type_}"
    now = int(time.time())
    ddb.put_item(
        TableName=table_name,
        Item={
            "pk": {"S": pk},
            "last_run_iso": {"S": iso_ts},
            "updated_at": {"N": str(now)},
        }
    )
    return pk
