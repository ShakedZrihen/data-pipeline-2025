import os
from botocore.config import Config

S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'http://localstack:4566')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
S3_BUCKET = os.getenv('S3_BUCKET', 'test-bucket')

QUEUE_BACKEND = os.getenv('QUEUE_BACKEND', 'rabbit').lower()
RABBIT_URL = os.getenv('RABBIT_URL', 'amqp://guest:guest@rabbitmq:5672/%2f')
RABBIT_QUEUE = os.getenv('RABBIT_QUEUE', 'results-queue')

STATE_BACKEND = os.getenv('STATE_BACKEND', 'mongo').lower()
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017')
MONGO_DB = os.getenv('MONGO_DB', 'prices')
MONGO_COL = os.getenv('MONGO_COL', 'last_run')

BOTO_CONFIG = Config(s3={"addressing_style": "path"})
