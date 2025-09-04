import json
import logging
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from typing import Dict, Any

from config import config

logger = logging.getLogger(__name__)


class SQSProducer:
    """Handle SQS message production"""
    
    def __init__(self):
        """Initialize SQS client and queue URL"""
        try:
            self.sqs = boto3.client('sqs', region_name=config.aws_region)
            self.queue_url = config.sqs_queue_url
            print(f"[INFO] SQS Producer initialized with queue: {self.queue_url}")
        except Exception as e:
            print(f"[ERROR] Failed to initialize SQS client: {str(e)}")
            self.sqs = None
            self.queue_url = None
    
    def send_message(self, message_data: Dict[str, Any]) -> bool:
        """
        Send message to SQS queue, automatically batching if message is too large
        
        Args:
            message_data: Message data to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.sqs or not self.queue_url:
            print(f"[ERROR] SQS client not properly initialized")
            return False
        
        try:
            # Check message size first
            message_body = json.dumps(message_data, ensure_ascii=False)
            message_size = len(message_body.encode('utf-8'))
            
            # SQS message limit is 256KB (262,144 bytes)
            max_size = 250000  # Leave some buffer for attributes
            
            if message_size <= max_size:
                # Message is small enough, send as-is
                return self._send_single_message(message_data, message_body)
            else:
                # Message too large, split into batches
                print(f"[INFO] Message size {message_size} bytes exceeds limit. Splitting into batches...")
                return self._send_large_message_in_batches(message_data)
            
        except Exception as e:
            print(f"[ERROR] Unexpected error processing message: {str(e)}")
            return False
    
    def _send_single_message(self, message_data: Dict[str, Any], message_body: str = None) -> bool:
        """Send a single message to SQS"""
        try:
            if not message_body:
                message_body = json.dumps(message_data, ensure_ascii=False)
            
            # Create message attributes for filtering/routing
            message_attributes = {
                'provider': {
                    'StringValue': message_data.get('provider', 'unknown'),
                    'DataType': 'String'
                },
                'type': {
                    'StringValue': message_data.get('type', 'unknown'),
                    'DataType': 'String'
                },
                'branch': {
                    'StringValue': message_data.get('branch', 'unknown'),
                    'DataType': 'String'
                }
            }
            
            # Send message to SQS
            response = self.sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_body,
                MessageAttributes=message_attributes,
                **({"MessageGroupId": f"{message_data.get('provider', 'default')}-{message_data.get('type', 'default')}"} if config.sqs_fifo_queue else {}),
                **({"MessageDeduplicationId": self._generate_dedup_id(message_data)} if config.sqs_fifo_queue else {})
            )
            
            message_id = response.get('MessageId')
            print(f"[SUCCESS] Successfully sent message to SQS: {message_id}")
            print(f"[SUCCESS] Message contains {len(message_data.get('items', []))} items from {message_data.get('provider', 'unknown')}")
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            print(f"[ERROR] AWS SQS ClientError [{error_code}]: {error_message}")
            return False
            
        except BotoCoreError as e:
            print(f"[ERROR] AWS SQS BotoCoreError: {str(e)}")
            return False
            
        except Exception as e:
            print(f"[ERROR] Unexpected error sending message to SQS: {str(e)}")
            return False
    
    def _send_large_message_in_batches(self, message_data: Dict[str, Any]) -> bool:
        """Split large message into smaller batches and send each"""
        items = message_data.get('items', [])
        if not items:
            return self._send_single_message(message_data)
        
        # Calculate optimal batch size by testing message sizes
        batch_size = 100  # Start with 100 items
        max_size = 250000
        
        # Test with a small batch to estimate size per item
        test_batch = message_data.copy()
        test_batch['items'] = items[:min(10, len(items))]
        test_message = json.dumps(test_batch, ensure_ascii=False)
        size_per_item = len(test_message.encode('utf-8')) / len(test_batch['items'])
        
        # Calculate safe batch size
        base_message_size = len(json.dumps({k: v for k, v in message_data.items() if k != 'items'}, ensure_ascii=False).encode('utf-8'))
        safe_batch_size = max(1, int((max_size - base_message_size) / size_per_item * 0.8))  # 80% buffer
        
        print(f"[INFO] Splitting {len(items)} items into batches of ~{safe_batch_size} items")
        
        successful_batches = 0
        total_batches = 0
        
        # Split items into batches
        for i in range(0, len(items), safe_batch_size):
            batch_items = items[i:i + safe_batch_size]
            total_batches += 1
            
            # Create batch message
            batch_message = message_data.copy()
            batch_message['items'] = batch_items
            batch_message['batch_info'] = {
                'batch_number': total_batches,
                'total_items_in_batch': len(batch_items),
                'original_total_items': len(items)
            }
            
            # Add unique suffix for FIFO deduplication
            if config.sqs_fifo_queue:
                batch_message['batch_timestamp'] = f"{message_data.get('timestamp', '')}-batch-{total_batches}"
            
            if self._send_single_message(batch_message):
                successful_batches += 1
                print(f"[SUCCESS] Sent batch {total_batches} with {len(batch_items)} items")
            else:
                print(f"[ERROR] Failed to send batch {total_batches}")
        
        success = successful_batches == total_batches
        print(f"[INFO] Batch sending complete: {successful_batches}/{total_batches} batches successful")
        return success
    
    def _generate_dedup_id(self, message_data: Dict[str, Any]) -> str:
        """
        Generate message deduplication ID for FIFO queues
        
        Args:
            message_data: Message data
            
        Returns:
            Deduplication ID string
        """
        provider = message_data.get('provider', 'unknown')
        branch = message_data.get('branch', 'unknown')
        file_type = message_data.get('type', 'unknown')
        
        # Use batch_timestamp if available (for batched messages), otherwise use regular timestamp
        timestamp = message_data.get('batch_timestamp', message_data.get('timestamp', 'unknown'))
        
        # Create unique ID based on provider, branch, type, and timestamp
        dedup_id = f"{provider}-{branch}-{file_type}-{timestamp}".replace(' ', '-')
        
        # SQS deduplication ID has max length of 128 chars
        if len(dedup_id) > 128:
            dedup_id = dedup_id[:128]
        
        return dedup_id
    
    def send_batch_messages(self, messages: list) -> int:
        """
        Send multiple messages in batch (up to 10 messages per batch)
        
        Args:
            messages: List of message data dictionaries
            
        Returns:
            Number of successfully sent messages
        """
        if not self.sqs or not self.queue_url:
            print(f"[ERROR] SQS client not properly initialized")
            return 0
        
        successful_count = 0
        
        # Process messages in batches of 10 (SQS limit)
        batch_size = 10
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            
            try:
                entries = []
                for j, message_data in enumerate(batch):
                    entry = {
                        'Id': str(i + j),
                        'MessageBody': json.dumps(message_data, ensure_ascii=False),
                        'MessageAttributes': {
                            'provider': {
                                'StringValue': message_data.get('provider', 'unknown'),
                                'DataType': 'String'
                            },
                            'type': {
                                'StringValue': message_data.get('type', 'unknown'),
                                'DataType': 'String'
                            }
                        }
                    }
                    
                    if config.sqs_fifo_queue:
                        entry['MessageGroupId'] = f"{message_data.get('provider', 'default')}-{message_data.get('type', 'default')}"
                        entry['MessageDeduplicationId'] = self._generate_dedup_id(message_data)
                    
                    entries.append(entry)
                
                response = self.sqs.send_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )
                
                successful_count += len(response.get('Successful', []))
                
                if response.get('Failed'):
                    for failed in response['Failed']:
                        print(f"[ERROR] Failed to send batch message {failed['Id']}: {failed['Message']}")
                        
            except Exception as e:
                print(f"[ERROR] Error sending message batch: {str(e)}")
        
        print(f"[SUCCESS] Successfully sent {successful_count} out of {len(messages)} messages")
        return successful_count