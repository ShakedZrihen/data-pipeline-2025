from typing import Iterable, Optional
from shared.config import settings
import boto3
from botocore.client import Config

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4")
    )

def list_objects(prefix: str = ""):
    s3 = s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def upload_bytes(key: str, data: bytes, content_type: str = "application/octet-stream"):
    s3 = s3_client()
    s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=data, ContentType=content_type)

def get_object_bytes(key: str) -> Optional[bytes]:
    s3 = s3_client()
    try:
        obj = s3.get_object(Bucket=settings.s3_bucket, Key=key)
        return obj["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
