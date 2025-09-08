import os
import boto3


BUCKET_NAME = os.getenv("BUCKET_NAME")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE")
QUEUE_NAME = os.getenv("QUEUE_NAME")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
INTERVAL_MIN = float(os.getenv("EXTRACT_EVERY_MINUTES", "60"))
REGION = os.getenv('REGION')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
ENDPOINT_URL = os.getenv('ENDPOINT_URL')


s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)

sqs = boto3.client(
    'sqs',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)

dynamodb = boto3.resource(
    'dynamodb', 
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)
