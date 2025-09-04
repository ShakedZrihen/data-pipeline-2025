import boto3, gzip, zipfile, io, logging
import os
import boto3
import gzip
import zipfile
import io
import logging
from botocore.config import Config
import re
import os

def provider_from_key(key: str) -> str:

    return key.split('/')[0] if '/' in key else "unknown"


def branch_from_key(key: str) -> str:

    filename = os.path.basename(key)
    match = re.search(r'-(\d{3})-', filename)
    if match:
        return match.group(1)
    return "000"


s3 = boto3.client('s3', endpoint_url=os.getenv("AWS_ENDPOINT_URL") or os.getenv("S3_ENDPOINT"))

GZIP_MAGIC = b'\x1f\x8b'
ZIP_MAGIC = b'PK'

def get_object_bytes(bucket: str, key: str) -> bytes:
    r = s3.get_object(Bucket=bucket, Key=key)
    return r['Body'].read()

def inflate_bytes(blob: bytes) -> bytes:
    try:
        if blob.startswith(GZIP_MAGIC):
            with gzip.GzipFile(fileobj=io.BytesIO(blob)) as gz:
                return gz.read()
        if blob.startswith(ZIP_MAGIC):
            with zipfile.ZipFile(io.BytesIO(blob)) as zf:
                names = zf.namelist()
                if not names:
                    return blob
                xml_candidates = [n for n in names if n.lower().endswith((".xml", ".txt"))]
                name = xml_candidates[0] if xml_candidates else names[0]
                return zf.read(name)
        return blob
    except Exception:
        logging.exception("Inflation failed")
        raise

def read_and_decompress_gz(bucket: str, key: str) -> bytes:
    return inflate_bytes(get_object_bytes(bucket, key))
