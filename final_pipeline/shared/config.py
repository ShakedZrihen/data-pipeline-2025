import os

class Settings:
    def __init__(self) -> None:
        self.s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        self.s3_bucket = os.getenv("S3_BUCKET", "raw-prices")
        self.s3_access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
        self.s3_secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
        self.database_url = os.getenv("DATABASE_URL")
        self.rabbitmq_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
        self.rabbitmq_queue = os.getenv("RABBITMQ_QUEUE", "prices_queue")
        self.mongo_url = os.getenv("MONGO_URL", "mongodb://mongodb:27017")
        self.api_title = os.getenv("API_TITLE", "Supermarket Prices API")
        self.api_version = os.getenv("API_VERSION", "1.0.0")

settings = Settings()
