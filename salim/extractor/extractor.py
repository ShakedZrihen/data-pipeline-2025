import json
import logging
import traceback
from typing import Dict, Any, List
from datetime import datetime
from urllib.parse import unquote

from config import config
from s3_handler import S3Handler
from file_processor import FileProcessor
from data_normalizer import DataNormalizer
from sqs_producer import SQSProducer
from dynamodb_tracker import DynamoDBTracker

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ExtractorLambda:
    """Main Lambda handler for processing S3 uploaded files"""
    
    def __init__(self):
        self.s3_handler = S3Handler()
        self.file_processor = FileProcessor()
        self.data_normalizer = DataNormalizer()
        self.sqs_producer = SQSProducer()
        self.db_tracker = DynamoDBTracker()
        
    def handler(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """
        Main Lambda handler function
        
        Args:
            event: S3 event notification
            context: Lambda context object
            
        Returns:
            Response dictionary with processing results
        """
        try:
            # Validate configuration
            config.validate()
            
            print(f"Processing S3 event with {len(event.get('Records', []))} records")
            
            results = []
            for record in event.get('Records', []):
                try:
                    result = self.process_s3_record(record)
                    results.append(result)
                except Exception as e:
                    print(f"Failed to process record: {record}")
                    print(f"Error: {str(e)}")
                    print(f"Traceback: {traceback.format_exc()}")
                    # Continue with other records (fault-tolerant)
                    results.append({
                        'status': 'error',
                        'error': str(e),
                        'record': record.get('s3', {}).get('object', {}).get('key', 'unknown')
                    })
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Processing completed',
                    'processed': len([r for r in results if r.get('status') == 'success']),
                    'errors': len([r for r in results if r.get('status') == 'error']),
                    'results': results
                })
            }
            
        except Exception as e:
            print(f"[ERROR] Lambda handler failed: {str(e)}")
            print(f"[ERROR] Traceback: {traceback.format_exc()}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': str(e),
                    'message': 'Lambda execution failed'
                })
            }
    
    def process_s3_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single S3 record
        
        Args:
            record: S3 event record
            
        Returns:
            Processing result dictionary
        """
        s3_info = record.get('s3', {})
        bucket = s3_info.get('bucket', {}).get('name')
        key = s3_info.get('object', {}).get('key')
        
        print(f"[INFO] Processing file: s3://{bucket}/{key}")
        
        # Parse file path to extract metadata (this will decode the URL)
        file_metadata = self.parse_s3_key(key)
        if not file_metadata:
            raise ValueError(f"Invalid S3 key format: {key}")
        
        print(f"[INFO] File metadata: {file_metadata}")
        
        # Download file from S3 - try both original and decoded keys
        file_content = self.s3_handler.download_file(bucket, key)
        if not file_content:
            # Try with decoded key if original fails
            decoded_key = file_metadata.get('key')  # This is the decoded version
            print(f"[INFO] Retrying download with decoded key: {decoded_key}")
            file_content = self.s3_handler.download_file(bucket, decoded_key)
            
        if not file_content:
            # Try with + replaced by spaces in decoded key
            space_key = decoded_key.replace('+', ' ') if decoded_key else None
            if space_key and space_key != decoded_key:
                print(f"[INFO] Retrying download with space-replaced key: {space_key}")
                file_content = self.s3_handler.download_file(bucket, space_key)
            
        if not file_content:
            space_key = decoded_key.replace('+', ' ') if decoded_key else 'none'
            raise ValueError(f"Failed to download file from S3: s3://{bucket}/{key} (tried decoded: {decoded_key}, tried with spaces: {space_key})")
        
        # Process the file (decompress and parse)
        start_time = datetime.now()
        processed_data = self.file_processor.process_file(file_content, file_metadata)
        if not processed_data:
            raise ValueError(f"Failed to process file content: {key}")
        
        # Normalize the data
        normalized_data = self.data_normalizer.normalize_data(processed_data, file_metadata)
        if not normalized_data:
            raise ValueError(f"Failed to normalize data: {key}")
        
        # Send to SQS queue
        sqs_success = self.sqs_producer.send_message(normalized_data)
        if not sqs_success:
            print(f"[WARNING] Failed to send message to SQS for file: {key}")
        
        # Update last run time in DynamoDB
        processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
        items_count = len(normalized_data.get('items', []))
        
        self.db_tracker.update_last_run(
            file_metadata['provider'], 
            file_metadata['branch'], 
            file_metadata['type'], 
            datetime.utcnow()
        )
        
        # Record processing stats
        self.db_tracker.record_processing_stats(
            file_metadata['provider'],
            file_metadata['branch'], 
            file_metadata['type'],
            items_count,
            processing_time,
            True
        )
        
        return {
            'status': 'success',
            'file': key,
            'metadata': file_metadata,
            'items_processed': items_count,
            'processing_time_ms': processing_time,
            'sqs_sent': sqs_success,
            'message': f'[SUCCESS] Successfully processed {items_count} items'
        }
    
    def parse_s3_key(self, key: str) -> Dict[str, str]:
        """
        Parse S3 key to extract provider, branch, type, and timestamp
        
        Expected format: providers/<provider>/<branch>/<type>_<timestamp>.gz
        
        Args:
            key: S3 object key
            
        Returns:
            Dictionary with parsed metadata or None if invalid
        """
        try:
            # URL decode the key to handle Hebrew characters and special chars
            decoded_key = unquote(key)
            parts = decoded_key.split('/')
            if len(parts) != 4 or not parts[0] == 'providers':
                return None
            
            provider = parts[1]
            branch = parts[2]
            filename = parts[3]
            
            # Parse filename: PriceFull202509020930.gz or PromoFull202509020930.gz
            if not filename.endswith('.gz'):
                return None
            
            name_part = filename[:-3]  # Remove .gz
            
            # Extract file type and timestamp from formats like:
            # PriceFull202509020930, PromoFull202509020930
            if name_part.lower().startswith('pricefull'):
                file_type = 'pricesFull'
                timestamp = name_part[9:]  # Remove 'PriceFull'
            elif name_part.lower().startswith('promofull'):
                file_type = 'promoFull'  
                timestamp = name_part[9:]  # Remove 'PromoFull'
            else:
                # Fallback: try underscore format
                if '_' not in name_part:
                    return None
                file_type, timestamp = name_part.rsplit('_', 1)
                if file_type not in ['pricesFull', 'promoFull']:
                    return None
            
            return {
                'provider': provider,
                'branch': branch,
                'type': file_type,
                'timestamp': timestamp,
                'key': decoded_key,
                'original_key': key,  # Keep original encoded key for S3 operations
                'filename': filename
            }
            
        except Exception as e:
            print(f"[ERROR] Error parsing S3 key '{key}': {str(e)}")
            return None


# Lambda entry point
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point"""
    extractor = ExtractorLambda()
    return extractor.handler(event, context)


# For local testing
if __name__ == "__main__":
    # Example S3 event for testing
    test_event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "supermarket-crawler"},
                    "object": {"key": "providers/yohananof/חדרה/pricesFull_20250902093000.gz"}
                }
            }
        ]
    }
    
    extractor = ExtractorLambda()
    result = extractor.handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))