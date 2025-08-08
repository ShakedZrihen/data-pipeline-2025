"""
Main Lambda Function for Price/Promo Data Extractor
Processes .gz files from S3, extracts and normalizes data, sends to SQS
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any

# Import our modular components
from src.s3_handler import S3Handler
from src.xml_processor import XMLProcessor
from src.normalizer import DataNormalizer
from src.sqs_producer import SQSProducer
from src.db_handler import DatabaseHandler

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize components
s3_handler = S3Handler()
xml_processor = XMLProcessor()
normalizer = DataNormalizer()
sqs_producer = SQSProducer()

# Initialize database handler
db_type = os.getenv('DB_TYPE', 'local')  # 'local', 'mongodb', or 'dynamodb'
db_handler = DatabaseHandler(db_type)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler triggered by S3 events
    
    Args:
        event: S3 event containing bucket and key information
        context: Lambda context
        
    Returns:
        Response with processing status
    """
    try:
        logger.info(f"Processing event: {json.dumps(event)}")
        
        # Process each S3 record in the event
        results = []
        for record in event.get('Records', []):
            result = process_s3_record(record)
            results.append(result)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing completed successfully',
                'processed_files': len(results),
                'results': results
            })
        }
        
    except Exception as e:
        logger.error(f"Lambda processing failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Processing failed'
            })
        }


def process_s3_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single S3 record
    
    Args:
        record: S3 event record
        
    Returns:
        Processing result
    """
    try:
        # Extract S3 information
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        logger.info(f"Processing file: s3://{bucket}/{key}")
        
        # Parse S3 key to get metadata
        provider, branch, file_type, timestamp = s3_handler.parse_s3_key(key)
        
        # Download file from S3
        gz_content = s3_handler.download_file(bucket, key)
        
        # Decompress the file
        xml_content = s3_handler.decompress_gz_file(gz_content)
        
        # Parse XML content
        parsed_data = xml_processor.parse(xml_content, provider, file_type)
        
        # Normalize the data
        normalized_items = normalizer.normalize(
            parsed_data, 
            provider, 
            branch, 
            file_type
        )
        
        # Create message for SQS
        message = {
            "provider": provider,
            "branch": branch,
            "type": file_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "items": normalized_items,
            "source_file": key,
            "items_count": len(normalized_items)
        }
        
        # Save JSON locally if in development/testing
        if os.getenv('SAVE_LOCAL_JSON', 'false').lower() == 'true':
            save_json_locally(message, provider, branch, file_type)
        
        # Send to SQS
        if os.getenv('SQS_QUEUE_URL'):
            response = sqs_producer.send_message(message)
            logger.info(f"Message sent to SQS: {response['MessageId']}")
        else:
            logger.warning("SQS_QUEUE_URL not configured, skipping SQS send")
        
        # Update last run timestamp in database
        if db_handler.update_last_run(provider, branch, file_type):
            logger.info(f"Updated last run timestamp for {provider}/{branch}/{file_type}")
        else:
            logger.warning("Failed to update last run timestamp")
        
        return {
            'file': key,
            'provider': provider,
            'branch': branch,
            'type': file_type,
            'items_processed': len(normalized_items),
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Failed to process record: {e}")
        return {
            'file': record.get('s3', {}).get('object', {}).get('key', 'unknown'),
            'status': 'failed',
            'error': str(e)
        }


def save_json_locally(data: Dict[str, Any], provider: str, 
                      branch: str, file_type: str) -> None:
    """
    Save JSON output locally for testing/review
    
    Args:
        data: JSON data to save
        provider: Provider name
        branch: Branch name
        file_type: File type (prices/promo)
    """
    try:
        # Create output directory
        output_dir = Path('/tmp/extractor_output')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{provider}_{branch}_{file_type}_{timestamp}.json"
        filepath = output_dir / filename
        
        # Save JSON
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON saved locally: {filepath}")
        
    except Exception as e:
        logger.error(f"Failed to save JSON locally: {e}")


# For local testing
if __name__ == "__main__":
    # Test event
    test_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "salim-prices"
                    },
                    "object": {
                        "key": "providers/victory/tel-aviv/pricesFull_20250808_120000.gz"
                    }
                }
            }
        ]
    }
    
    # Set environment for local testing
    os.environ['SAVE_LOCAL_JSON'] = 'true'
    
    # Run handler
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
