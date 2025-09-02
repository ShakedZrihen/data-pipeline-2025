import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration settings for the extractor Lambda"""
    
    # AWS Settings
    aws_region: str = os.getenv('AWS_DEFAULT_REGION', 'eu-west-1')
    sqs_queue_url: str = os.getenv('SQS_QUEUE_URL', '')
    sqs_fifo_queue: bool = os.getenv('SQS_FIFO_QUEUE', 'false').lower() == 'true'
    dynamodb_table_name: str = os.getenv('DYNAMODB_TABLE_NAME', 'supermarket-extractor-tracking')
    
    # S3 Settings
    s3_bucket: str = os.getenv('S3_BUCKET', 'supermarket-crawler')
    
    # Processing Settings
    max_items_per_file: int = int(os.getenv('MAX_ITEMS_PER_FILE', '10000'))
    enable_local_save: bool = os.getenv('ENABLE_LOCAL_SAVE', 'false').lower() == 'true'
    local_output_dir: str = os.getenv('LOCAL_OUTPUT_DIR', '/tmp/extractor_output')
    
    # Logging
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    
    def validate(self) -> bool:
        """Validate required configuration"""
        if not self.sqs_queue_url:
            raise ValueError("SQS_QUEUE_URL environment variable is required")
        if not self.dynamodb_table_name:
            raise ValueError("DYNAMODB_TABLE_NAME environment variable is required")
        return True


# Global config instance
config = Config()