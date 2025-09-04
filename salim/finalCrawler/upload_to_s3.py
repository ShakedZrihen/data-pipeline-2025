import os, sys
from pathlib import Path
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
BUCKET = "ingest-bucket"

ROOT_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(r".\providers")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id=AWS_KEY,
    aws_secret_access_key=AWS_SECRET,
    config=Config(s3={"addressing_style": "path"})
)

def ensure_bucket(bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"[ok] Bucket exists: s3://{bucket}")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in {"404", "NoSuchBucket"}:
            print(f"[+] Creating bucket: s3://{bucket}")
            s3.create_bucket(Bucket=bucket)
        else:
            raise

def find_gzs(root: Path):
    if not root.exists():
        print(f"ERROR: folder not found: {root.resolve()}")
        return []
    gzs = []
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() == ".gz":
            gzs.append(p)
    return gzs

def upload(path: Path):
    rel = path.relative_to(ROOT_DIR)
    parts = rel.parts
    provider = parts[0] if len(parts) >= 2 else "misc"
    key = str(Path(provider) / Path(path.name)).replace("\\", "/")
    print(f"Uploading: {path} -> s3://{BUCKET}/{key}")
    s3.upload_file(str(path), BUCKET, key)

def list_bucket():
    resp = s3.list_objects_v2(Bucket=BUCKET)
    for obj in resp.get("Contents", []):
        print(f" - {obj['Key']} ({obj['Size']} bytes)")

if __name__ == "__main__":
    print(f"Scanning: {ROOT_DIR.resolve()}")
    ensure_bucket(BUCKET)
    files = find_gzs(ROOT_DIR)
    print(f"Found {len(files)} gz(s).")
    for f in files:
        upload(f)
    print("\nBucket contents:")
    list_bucket()
