import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQSConsumer:
    """Lambda function to consume SQS messages and save as JSON files"""
    
    def __init__(self):
        print("Starting the processing")
        self.output_dir = "/tmp/json_files"
        self.ensure_output_directory()
    
    def ensure_output_directory(self):
        """Ensure the output directory exists"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"Created output directory: {self.output_dir}")
    
    def handler(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """
        Main Lambda handler for processing SQS messages
        
        Args:
            event: SQS event with messages
            context: Lambda context object
            
        Returns:
            Processing results
        """
        try:
            print(f"[SUCCESS] SQS Consumer started")
            print(f"[INFO] Processing {len(event.get('Records', []))} SQS messages")
            
            results = []
            
            for record in event.get('Records', []):
                try:
                    result = self.process_sqs_message(record)
                    results.append(result)
                    print(f"[SUCCESS] Processed message: {result['file_name']}")
                except Exception as e:
                    print(f"[ERROR] Failed to process SQS record: {str(e)}")
                    results.append({
                        'status': 'error',
                        'error': str(e),
                        'message_id': record.get('messageId', 'unknown')
                    })
            
            successful_count = len([r for r in results if r.get('status') == 'success'])
            error_count = len([r for r in results if r.get('status') == 'error'])
            
            print(f"[SUCCESS] Completed processing: {successful_count} successful, {error_count} errors")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'SQS messages processed successfully',
                    'processed': successful_count,
                    'errors': error_count,
                    'results': results
                })
            }
            
        except Exception as e:
            print(f"[ERROR] Lambda handler failed: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': str(e),
                    'message': 'Lambda execution failed'
                })
            }
    
    def process_sqs_message(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single SQS message and save as JSON file
        
        Args:
            record: SQS message record
            
        Returns:
            Processing result
        """
        try:
            # Extract message data
            message_body = record.get('body', '{}')
            message_id = record.get('messageId', 'unknown')
            receipt_handle = record.get('receiptHandle', 'unknown')
            
            print(f"[INFO] Processing message ID: {message_id}")
            
            # Parse JSON message
            try:
                message_data = json.loads(message_body)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in message body: {str(e)}")
            
            # Generate filename based on message content
            file_name = self.generate_filename(message_data, message_id)
            file_path = os.path.join(self.output_dir, file_name)
            
            # Create enhanced JSON structure with metadata
            enhanced_data = {
                'message_metadata': {
                    'message_id': message_id,
                    'receipt_handle': receipt_handle,
                    'processed_at': datetime.utcnow().isoformat() + 'Z',
                    'processor': 'sqs-consumer-lambda'
                },
                'supermarket_data': message_data
            }
            
            # Save to JSON file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(enhanced_data, f, indent=2, ensure_ascii=False)
            
            file_size = os.path.getsize(file_path)
            
            print(f"[SUCCESS] Saved JSON file: {file_name} ({file_size} bytes)")
            
            # Print a concise summary instead of full JSON content
            items_count = len(message_data.get('items', []))
            print(f"[SUMMARY] Provider: {message_data.get('provider')}, Branch: {message_data.get('branch')}, Type: {message_data.get('type')}, Items: {items_count}, File: {file_name}")
            
            return {
                'status': 'success',
                'message_id': message_id,
                'file_name': file_name,
                'file_path': file_path,
                'file_size_bytes': file_size,
                'items_count': len(message_data.get('items', [])),
                'provider': message_data.get('provider', 'unknown'),
                'branch': message_data.get('branch', 'unknown'),
                'type': message_data.get('type', 'unknown')
            }
            
        except Exception as e:
            print(f"[ERROR] Error processing SQS message: {str(e)}")
            raise
    
    def generate_filename(self, message_data: Dict[str, Any], message_id: str) -> str:
        """
        Generate a filename based on message content
        
        Args:
            message_data: Parsed message data
            message_id: SQS message ID
            
        Returns:
            Generated filename
        """
        try:
            # Extract key information
            provider = message_data.get('provider', 'unknown').replace('/', '_')
            branch = message_data.get('branch', 'unknown').replace('/', '_').replace(' ', '_')
            file_type = message_data.get('type', 'unknown')
            timestamp = message_data.get('timestamp', '').replace(':', '').replace('-', '').replace('T', '_').replace('Z', '')
            
            # Clean branch name (remove Hebrew characters that might cause issues)
            import re
            branch_clean = re.sub(r'[^\w\-_.]', '_', branch)
            
            # Generate filename
            if timestamp:
                filename = f"{provider}_{branch_clean}_{file_type}_{timestamp}.json"
            else:
                current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                filename = f"{provider}_{branch_clean}_{file_type}_{current_time}_{message_id[:8]}.json"
            
            return filename
            
        except Exception as e:
            print(f"[WARNING] Error generating filename, using fallback: {str(e)}")
            current_time = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            return f"supermarket_data_{current_time}_{message_id[:8]}.json"


# Lambda entry point
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point"""
    consumer = SQSConsumer()
    return consumer.handler(event, context)


# For local testing
if __name__ == "__main__":
    # Example SQS event for testing
    test_event = {
        "Records": [
            {
                "messageId": "12345-67890-abcdef",
                "receiptHandle": "AQEBwJnKy...",
                "body": json.dumps({
                    "provider": "Keshet",
                    "branch": "קולינריק חורב",
                    "type": "pricesFull",
                    "timestamp": "2025-09-02T14:30:00Z",
                    "items": [
                        {
                            "product": "מיץ עגבניות 1ל",
                            "price": 6.6,
                            "unit": "liter"
                        }
                    ]
                }),
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "...",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:eu-west-1:123456789:supermarket-queue",
                "awsRegion": "eu-west-1"
            }
        ]
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False))