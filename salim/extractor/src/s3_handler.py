"""
S3 Handler Module
Downloads and processes files from S3
"""

import gzip
import boto3
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Handler:
    """
    Handles S3 operations for the Lambda function
    """
    
    def __init__(self, region: str = 'il-central-1'):
        """
        Initialize S3 handler
        
        Args:
            region: AWS region
        """
        self.s3_client = boto3.client('s3', region_name=region)
    
    def download_file(self, bucket: str, key: str, 
                     local_path: Optional[str] = None) -> bytes:
        """
        Download file from S3 and return its contents
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            local_path: Optional path to save file locally
            
        Returns:
            File contents as bytes
        """
        try:
            logger.info(f"Downloading s3://{bucket}/{key}")
            
            # Download object
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            
            # Save locally if path provided
            if local_path:
                Path(local_path).parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, 'wb') as f:
                    f.write(content)
                logger.info(f"Saved to {local_path}")
            
            return content
            
        except ClientError as e:
            logger.error(f"Failed to download from S3: {e}")
            raise
    
    def decompress_gz_file(self, gz_content: bytes) -> bytes:
        """
        Decompress gzip content
        
        Args:
            gz_content: Gzipped content
            
        Returns:
            Decompressed content
        """
        try:
            return gzip.decompress(gz_content)
        except gzip.BadGzipFile as e:
            logger.error(f"Invalid gzip file: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to decompress: {e}")
            raise
    
    def parse_s3_key(self, key: str) -> Tuple[str, str, str, str]:
        """
        Parse S3 key to extract provider, branch, type, and timestamp
        
        Expected format: providers/<provider>/<branch>/<type>Full_<timestamp>.gz
        
        Args:
            key: S3 object key
            
        Returns:
            Tuple of (provider, branch, file_type, timestamp)
        """
        try:
            parts = key.split('/')
            
            if len(parts) < 4:
                raise ValueError(f"Invalid S3 key format: {key}")
            
            provider = parts[1]
            branch = parts[2]
            filename = parts[3]
            
            # Extract type and timestamp from filename
            # Format: <type>Full_<timestamp>.gz
            if filename.endswith('.gz'):
                filename = filename[:-3]  # Remove .gz
            
            if 'priceFull' in filename.lower():
                file_type = 'pricesFull'
            elif 'promoFull' in filename.lower():
                file_type = 'promoFull'
            else:
                file_type = 'unknown'
            
            # Extract timestamp
            timestamp = filename.split('_')[-1] if '_' in filename else 'unknown'
            
            logger.info(f"Parsed S3 key: provider={provider}, branch={branch}, "
                       f"type={file_type}, timestamp={timestamp}")
            
            return provider, branch, file_type, timestamp
            
        except Exception as e:
            logger.error(f"Failed to parse S3 key '{key}': {e}")
            raise
    
    def upload_file(self, bucket: str, key: str, content: bytes,
                   content_type: str = 'application/json') -> Dict:
        """
        Upload content to S3
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            content: Content to upload
            content_type: MIME type
            
        Returns:
            Response from S3
        """
        try:
            response = self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content,
                ContentType=content_type
            )
            logger.info(f"Uploaded to s3://{bucket}/{key}")
            return response
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise
    
    def list_files(self, bucket: str, prefix: str = '') -> list:
        """
        List files in S3 bucket with given prefix
        
        Args:
            bucket: S3 bucket name
            prefix: Key prefix to filter
            
        Returns:
            List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents']]
            
        except ClientError as e:
            logger.error(f"Failed to list S3 objects: {e}")
            raise
