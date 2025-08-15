import json
import logging
import boto3
from typing import Dict, Any
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class SQSProducer:    
    def __init__(self, queue_url: str, region_name: str = 'us-east-1'):
        self.queue_url = queue_url
        self.region_name = region_name
        self.sqs_client = None
        
    def _get_sqs_client(self):
        """Get SQS client, creating if needed"""
        if self.sqs_client is None:
            try:
                self.sqs_client = boto3.client('sqs', region_name=self.region_name)
                logger.info(f"Created SQS client for region: {self.region_name}")
            except Exception as e:
                logger.error(f"Failed to create SQS client: {e}")
                raise
        return self.sqs_client
    
    def send_message(self, message_data: Dict[str, Any]) -> bool:
        """Send a message to SQS queue"""
        try:
            sqs_client = self._get_sqs_client()
            
            # Convert message to JSON string
            message_body = json.dumps(message_data, ensure_ascii=False)
            
            # Send message to SQS
            response = sqs_client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_body,
                MessageAttributes={
                    'provider': {
                        'StringValue': message_data.get('provider', ''),
                        'DataType': 'String'
                    },
                    'type': {
                        'StringValue': message_data.get('type', ''),
                        'DataType': 'String'
                    },
                    'timestamp': {
                        'StringValue': message_data.get('timestamp', ''),
                        'DataType': 'String'
                    }
                }
            )
            
            logger.info(f"Successfully sent message to SQS. MessageId: {response['MessageId']}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to send message to SQS: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message to SQS: {e}")
            return False
    
    def send_batch_messages(self, messages: list[Dict[str, Any]]) -> Dict[str, int]:
        """Send multiple messages in batch (up to 10 per batch)"""
        if not messages:
            return {'success': 0, 'failed': 0}
        
        # SQS batch limit is 10 messages
        batch_size = 10
        total_success = 0
        total_failed = 0
        
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            
            try:
                sqs_client = self._get_sqs_client()
                
                # Prepare batch entries
                entries = []
                for j, message_data in enumerate(batch):
                    message_body = json.dumps(message_data, ensure_ascii=False)
                    
                    entry = {
                        'Id': f'msg_{i + j}',
                        'MessageBody': message_body,
                        'MessageAttributes': {
                            'provider': {
                                'StringValue': message_data.get('provider', ''),
                                'DataType': 'String'
                            },
                            'type': {
                                'StringValue': message_data.get('type', ''),
                                'DataType': 'String'
                            },
                            'timestamp': {
                                'StringValue': message_data.get('timestamp', ''),
                                'DataType': 'String'
                            }
                        }
                    }
                    entries.append(entry)
                
                # Send batch
                response = sqs_client.send_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )
                
                # Count successful and failed
                if 'Successful' in response:
                    total_success += len(response['Successful'])
                if 'Failed' in response:
                    total_failed += len(response['Failed'])
                
                logger.info(f"Batch {i//batch_size + 1}: {len(batch)} messages processed")
                
            except Exception as e:
                logger.error(f"Failed to send batch {i//batch_size + 1}: {e}")
                total_failed += len(batch)
        
        logger.info(f"Batch processing complete: {total_success} successful, {total_failed} failed")
        return {'success': total_success, 'failed': total_failed}
    
    def get_queue_attributes(self) -> Dict[str, Any]:
        """Get queue attributes for monitoring"""
        try:
            sqs_client = self._get_sqs_client()
            response = sqs_client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=['All']
            )
            return response.get('Attributes', {})
        except Exception as e:
            logger.error(f"Failed to get queue attributes: {e}")
            return {}
    
    def test_connection(self) -> bool:
        """Test if we can connect to SQS"""
        try:
            sqs_client = self._get_sqs_client()
            sqs_client.get_queue_attributes(QueueUrl=self.queue_url)
            logger.info("SQS connection test successful")
            return True
        except Exception as e:
            logger.error(f"SQS connection test failed: {e}")
            return False
