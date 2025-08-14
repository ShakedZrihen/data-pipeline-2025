import json
import logging
import os
from typing import Dict, Any
from pathlib import Path

from .file_processor import FileProcessor
from .data_normalizer import DataNormalizer
from .sqs_producer import SQSProducer
from .db_handler import DynamoDBHandler

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ExtractorLambda:
    
    def __init__(self):
        # Initialize components
        self.file_processor = FileProcessor()
        self.data_normalizer = DataNormalizer()
        
        # Get configuration from environment variables
        self.sqs_queue_url = os.environ.get('SQS_QUEUE_URL')
        self.dynamodb_table = os.environ.get('DYNAMODB_TABLE', 'extractor-last-runs')
        self.aws_region = os.environ.get('AWS_REGION', 'us-east-1')
        self.output_dir = os.environ.get('OUTPUT_DIR', '/tmp/extracted')
        
        # Initialize AWS services
        self.sqs_producer = SQSProducer(self.sqs_queue_url, self.aws_region)
        self.db_handler = DynamoDBHandler(self.dynamodb_table, self.aws_region)
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
    
    def process_s3_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Process S3 event and extract data from uploaded files"""
        try:
            logger.info(f"Processing S3 event: {json.dumps(event)}")
            
            # Extract S3 information from event
            s3_records = event.get('Records', [])
            if not s3_records:
                logger.warning("No S3 records found in event")
                return {'status': 'no_records', 'processed': 0}
            
            processed_files = 0
            failed_files = 0
            
            for record in s3_records:
                try:
                    # Extract S3 bucket and key
                    s3_bucket = record['s3']['bucket']['name']
                    s3_key = record['s3']['object']['key']
                    
                    logger.info(f"Processing file: s3://{s3_bucket}/{s3_key}")
                    
                    # Validate file type and path
                    if not self._validate_file_path(s3_key):
                        logger.warning(f"Skipping file with invalid path format: {s3_key}")
                        failed_files += 1
                        continue
                    
                    # Download file from S3 to local temp
                    local_file_path = self._download_from_s3(s3_bucket, s3_key)
                    
                    # Process the file
                    success = self._process_single_file(local_file_path, s3_key)
                    
                    if success:
                        processed_files += 1
                    else:
                        failed_files += 1
                        
                    # Clean up local file
                    self._cleanup_local_file(local_file_path)
                    
                except Exception as e:
                    logger.error(f"Failed to process S3 record: {e}")
                    failed_files += 1
                    continue
            
            result = {
                'status': 'completed',
                'processed': processed_files,
                'failed': failed_files,
                'total': len(s3_records)
            }
            
            logger.info(f"Processing complete: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to process S3 event: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def _download_from_s3(self, bucket: str, key: str) -> str:
        """Download file from S3 to local temp directory"""
        import boto3
        
        s3_client = boto3.client('s3')
        local_file_path = os.path.join(self.output_dir, os.path.basename(key))
        
        logger.info(f"Downloading s3://{bucket}/{key} to {local_file_path}")
        s3_client.download_file(bucket, key, local_file_path)
        
        return local_file_path
    
    def _process_single_file(self, local_file_path: str, s3_key: str) -> bool:
        """Process a single file and send to SQS"""
        try:
            # Extract filename from S3 key
            filename = os.path.basename(s3_key)
            
            # Parse S3 key path to extract provider and branch information
            # Expected format: providers/<provider>/<branch>/pricesFull_<timestamp>.gz
            # or providers/<provider>/<branch>/promoFull_<timestamp>.gz
            path_parts = s3_key.split('/')
            if len(path_parts) >= 4 and path_parts[0] == 'providers':
                provider_from_path = path_parts[1]
                branch_from_path = path_parts[2]
                logger.info(f"Extracted provider: {provider_from_path}, branch: {branch_from_path} from S3 path")
            else:
                provider_from_path = None
                branch_from_path = None
                logger.info(f"Could not parse provider/branch from S3 path: {s3_key}")
            
            # Process the file
            metadata, content = self.file_processor.process_file(local_file_path)
            
            # Normalize data to required JSON format
            normalized_data = self.data_normalizer.create_output_json(
                metadata, content, filename
            )
            
            # Override provider and branch with path information if available
            if provider_from_path and branch_from_path:
                normalized_data['provider'] = provider_from_path
                normalized_data['branch'] = branch_from_path
                logger.info(f"Updated provider to {provider_from_path} and branch to {branch_from_path}")
            
            # Save JSON locally for review
            json_filename = f"{Path(filename).stem}.json"
            json_path = os.path.join(self.output_dir, json_filename)
            self.data_normalizer.save_json_locally(normalized_data, json_path)
            
            # Send to SQS
            if self.sqs_producer.send_message(normalized_data):
                logger.info(f"Successfully sent data to SQS for {filename}")
                
                # Update last run time in DynamoDB
                provider = normalized_data['provider']
                branch = normalized_data['branch']
                file_type = normalized_data['type']
                timestamp = normalized_data['timestamp']
                
                self.db_handler.update_last_run_time(
                    provider, branch, file_type, timestamp
                )
                
                return True
            else:
                logger.error(f"Failed to send data to SQS for {filename}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to process file {local_file_path}: {e}")
            return False
    
    def _cleanup_local_file(self, local_file_path: str):
        """Clean up local temporary file"""
        try:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                logger.debug(f"Cleaned up local file: {local_file_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup local file {local_file_path}: {e}")
    
    def _validate_file_path(self, s3_key: str) -> bool:
        """Validate that the S3 key follows the expected format"""
        try:
            # Expected format: providers/<provider>/<branch>/pricesFull_<timestamp>.gz
            # or providers/<provider>/<branch>/promoFull_<timestamp>.gz
            path_parts = s3_key.split('/')
            
            # Check if it's a providers file
            if len(path_parts) < 4 or path_parts[0] != 'providers':
                logger.warning(f"File not in providers directory: {s3_key}")
                return False
            
            # Check if filename ends with .gz
            filename = path_parts[-1]
            if not filename.endswith('.gz'):
                logger.warning(f"File is not a .gz file: {filename}")
                return False
            
            # Check if filename contains expected patterns
            if not any(pattern in filename for pattern in ['pricesFull', 'promoFull']):
                logger.warning(f"File doesn't match expected naming pattern: {filename}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating file path {s3_key}: {e}")
            return False
    
    def test_connections(self) -> Dict[str, bool]:
        """Test connections to AWS services"""
        results = {}
        
        # Test SQS
        if self.sqs_queue_url:
            results['sqs'] = self.sqs_producer.test_connection()
        else:
            results['sqs'] = False
            logger.warning("SQS_QUEUE_URL not configured")
        
        # Test DynamoDB
        results['dynamodb'] = self.db_handler.test_connection()
        
        # Test DynamoDB table creation
        if results['dynamodb']:
            results['dynamodb_table'] = self.db_handler.create_table_if_not_exists()
        
        return results

# Lambda handler function
def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        # Initialize extractor
        extractor = ExtractorLambda()
        
        # Test connections (useful for debugging)
        if os.environ.get('TEST_CONNECTIONS', 'false').lower() == 'true':
            connection_results = extractor.test_connections()
            logger.info(f"Connection test results: {connection_results}")
        
        # Process the event
        result = extractor.process_s3_event(event)
        
        return {
            'statusCode': 200,
            'body': json.dumps(result),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'error': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
