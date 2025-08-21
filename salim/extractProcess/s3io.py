import io, gzip, json, re
import boto3
from botocore.exceptions import ClientError
from .config import (
    S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BOTO_CONFIG
)

aws_cfg = dict(
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    config=BOTO_CONFIG,
)

s3 = boto3.client("s3", **aws_cfg)

def ensure_bucket(bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"[Init] Created S3 bucket: {bucket}")

def download_gz(bucket: str, key: str) -> io.BytesIO:
    # wanted to make sure it is gz file because earlier i got the html file instead and it was harbu darbu
    obj = s3.get_object(Bucket=bucket, Key=key)
    gz_bytes = obj["Body"].read()
    first16 = gz_bytes[:16]
    print(f"[S3] {key} first bytes: {first16!r}")
    if len(gz_bytes) < 2 or gz_bytes[:2] != b"\x1f\x8b":
        raise ValueError(f"Not a gz header: {first16!r}")
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes)) as gz:
        xml_bytes = gz.read()
    return io.BytesIO(xml_bytes)

def save_json(bucket: str, src_key: str, doc: dict) -> str:
    json_key = re.sub(r'\.gz$', '.json', src_key)
    body = json.dumps(doc, ensure_ascii=False).encode('utf-8')
    s3.put_object(Bucket=bucket, Key=json_key, Body=body, ContentType='application/json')
    return json_key
