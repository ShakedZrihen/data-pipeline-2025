import os
import re
import boto3

REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", "http://localstack:4566")

def client(service: str):
    return boto3.client(service, region_name=REGION, endpoint_url=AWS_ENDPOINT)

def parse_s3_key(key: str):
    parts = key.split("/")
    if len(parts) < 4 or parts[0] != "providers":
        raise ValueError(f"Bad key format: {key}")
    provider, branch, filename = parts[1], parts[2], parts[3]
    if not filename.endswith(".gz"):
        raise ValueError("Not a .gz file")
    base = filename[:-3]  # הסרת .gz
    m = re.match(r"^(pricesFull|promoFull)_(.+)$", base)
    if not m:
        raise ValueError(f"Bad filename format: {filename}")
    file_type, timestamp = m.group(1), m.group(2)
    return provider, branch, file_type, timestamp
