import boto3
import logging
from typing import Optional, BinaryIO
from botocore.exceptions import ClientError
from io import BytesIO

from config import config

logger = logging.getLogger(__name__)


class S3Handler:
    """Handle S3 operations for file processing"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3', region_name=config.aws_region)
        
    def download_file(self, bucket: str, key: str) -> Optional[BytesIO]:
        """
        Download file from S3 and return as BytesIO
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            BytesIO object with file content or None if failed
        """
        try:
            logger.info(f"Downloading s3://{bucket}/{key}")
            
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            file_content = response['Body'].read()
            
            logger.info(f"Downloaded {len(file_content)} bytes from s3://{bucket}/{key}")
            return BytesIO(file_content)
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                logger.error(f"File not found: s3://{bucket}/{key}")
            else:
                logger.error(f"Failed to download s3://{bucket}/{key}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading s3://{bucket}/{key}: {str(e)}")
            return None
    
    def file_exists(self, bucket: str, key: str) -> bool:
        """
        Check if file exists in S3
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking file existence s3://{bucket}/{key}: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error checking file existence s3://{bucket}/{key}: {str(e)}")
            return False
    
    def get_file_metadata(self, bucket: str, key: str) -> Optional[dict]:
        """
        Get file metadata from S3
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            Dictionary with file metadata or None if failed
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return {
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified'),
                'etag': response.get('ETag', '').strip('"'),
                'content_type': response.get('ContentType', ''),
                'metadata': response.get('Metadata', {})
            }
        except Exception as e:
            logger.error(f"Failed to get metadata for s3://{bucket}/{key}: {str(e)}")
            return None