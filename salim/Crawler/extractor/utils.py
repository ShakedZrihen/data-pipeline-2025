import os
import re
import boto3

REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", "http://localstack:4566")

def client(service: str):
    return boto3.client(service, region_name=REGION, endpoint_url=AWS_ENDPOINT)

def parse_s3_key(key: str):
    # Example key: 'keshet/021/PromoFull7290785400000-021-202508130010.gz'
    parts = key.split("/")
    provider, branch, filename = parts[0], parts[1], parts[2]

    if not filename.endswith(".gz"):
        raise ValueError(f"Not a .gz file: {filename}")
    
    base = filename[:-3] # remove .gz
    
    # This regex handles both PriceFull and PromoFull and extracts the final timestamp
    match = re.search(r"(?i)(Price|Promo)Full.*?-(\d{12,14})$", base)
    if not match:
        raise ValueError(f"Bad filename format: {filename}")
        
    file_type = match.group(1).lower() + "Full" # Standardize to 'priceFull' or 'promoFull'
    timestamp = match.group(2)
    
    return provider, branch, file_type, timestamp