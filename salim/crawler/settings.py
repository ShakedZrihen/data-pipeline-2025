import boto3
import os

# URLs and Provider Configuration
GOV_URL = os.getenv("GOV_URL")
PROVIDERS = {
    "יוחננוף": {"username": "yohananof", "password": "", "folder": "yohananof"},
    "חצי חינם": {"username": "", "password": "", "folder": "hatzi-hinam"},
    "ויקטורי": {"username": "", "password": "", "folder": "victory"},
}

# S3 Configuration
S3_BUCKET = os.getenv("BUCKET_NAME")

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("REGION")
)