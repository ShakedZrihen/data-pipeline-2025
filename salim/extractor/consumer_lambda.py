"""
Consumer Lambda Function
Reads messages from SQS and prints the JSON content
This is a simple implementation for the assignment requirement
"""

import json
import logging
from typing import Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler triggered by SQS messages
    Reads and prints JSON messages
    
    Args:
        event: SQS event containing messages
        context: Lambda context
        
    Returns:
        Response with processing status
    """
    try:
        logger.info(f"Received {len(event.get('Records', []))} messages from SQS")
        
        processed_messages = []
        
        # Process each SQS message
        for record in event.get('Records', []):
            # Get message body
            message_body = record.get('body', '{}')
            
            # Parse JSON
            try:
                message_data = json.loads(message_body)
                
                # Print the JSON content (main requirement)
                print("=" * 60)
                print("RECEIVED MESSAGE FROM SQS:")
                print("=" * 60)
                print(json.dumps(message_data, indent=2, ensure_ascii=False))
                print("=" * 60)
                
                # Log summary information
                logger.info(f"Provider: {message_data.get('provider')}")
                logger.info(f"Branch: {message_data.get('branch')}")
                logger.info(f"Type: {message_data.get('type')}")
                logger.info(f"Items Count: {message_data.get('items_count', 0)}")
                logger.info(f"Source File: {message_data.get('source_file')}")
                
                # Add to processed list
                processed_messages.append({
                    'messageId': record.get('messageId'),
                    'provider': message_data.get('provider'),
                    'branch': message_data.get('branch'),
                    'type': message_data.get('type'),
                    'items_count': message_data.get('items_count', 0),
                    'status': 'processed'
                })
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
                logger.error(f"Message body: {message_body}")
                processed_messages.append({
                    'messageId': record.get('messageId'),
                    'status': 'failed',
                    'error': 'Invalid JSON'
                })
        
        # Return success response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Messages processed successfully',
                'processed_count': len(processed_messages),
                'results': processed_messages
            })
        }
        
    except Exception as e:
        logger.error(f"Consumer Lambda failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Processing failed'
            })
        }


# For local testing
if __name__ == "__main__":
    # Test SQS event
    test_event = {
        "Records": [
            {
                "messageId": "test-message-1",
                "body": json.dumps({
                    "provider": "victory",
                    "branch": "tel-aviv",
                    "type": "pricesFull",
                    "timestamp": "2025-08-08T12:00:00Z",
                    "items_count": 294,
                    "source_file": "providers/victory/tel-aviv/pricesFull_20250808_120000.gz",
                    "items": [
                        {
                            "product": "חלב תנובה 3%",
                            "price": 5.9,
                            "unit": "liter",
                            "barcode": "7290000009358"
                        },
                        {
                            "product": "לחם אחיד פרוס",
                            "price": 7.5,
                            "unit": "unit",
                            "barcode": "7290000009359"
                        }
                    ]
                })
            }
        ]
    }
    
    # Run handler
    result = lambda_handler(test_event, None)
    print("\nLambda Response:")
    print(json.dumps(result, indent=2))
