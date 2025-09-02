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
        Send message to SQS queue
        
        Args:
            message_data: Message data to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.sqs or not self.queue_url:
            print(f"[ERROR] SQS client not properly initialized")
            return False
        
        try:
            # Convert message data to JSON
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
        timestamp = message_data.get('timestamp', 'unknown')
        
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