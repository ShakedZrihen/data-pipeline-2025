"""
SQS Producer Module
Sends normalized JSON messages to AWS SQS queue
"""

import json
import boto3
import logging
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class SQSProducer:
    """
    Handles sending messages to AWS SQS queue
    """
    
    def __init__(self, queue_url: Optional[str] = None, region: str = 'il-central-1'):
        """
        Initialize SQS producer
        
        Args:
            queue_url: SQS queue URL
            region: AWS region
        """
        self.sqs_client = boto3.client('sqs', region_name=region)
        self.queue_url = queue_url
        
        # If no queue URL provided, try to get from environment
        if not self.queue_url:
            import os
            self.queue_url = os.getenv('SQS_QUEUE_URL')
            
        if not self.queue_url:
            logger.warning("No SQS queue URL configured")
    
    def send_message(self, message_body: Dict[str, Any], 
                     message_attributes: Optional[Dict] = None) -> Dict:
        """
        Send a message to SQS queue
        
        Args:
            message_body: Dictionary to send as JSON
            message_attributes: Optional message attributes
            
        Returns:
            Response from SQS
        """
        if not self.queue_url:
            raise ValueError("SQS queue URL not configured")
        
        try:
            # Convert message to JSON string
            if isinstance(message_body, dict):
                message_json = json.dumps(message_body, ensure_ascii=False)
            else:
                message_json = str(message_body)
            
            # Prepare message attributes
            attributes = {}
            if message_attributes:
                for key, value in message_attributes.items():
                    attributes[key] = {
                        'StringValue': str(value),
                        'DataType': 'String'
                    }
            
            # Add default attributes
            if 'provider' in message_body:
                attributes['Provider'] = {
                    'StringValue': message_body['provider'],
                    'DataType': 'String'
                }
            
            if 'type' in message_body:
                attributes['FileType'] = {
                    'StringValue': message_body['type'],
                    'DataType': 'String'
                }
            
            # Send message
            response = self.sqs_client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=message_json,
                MessageAttributes=attributes
            )
            
            logger.info(f"Message sent to SQS. MessageId: {response['MessageId']}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to send message to SQS: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error sending to SQS: {e}")
            raise
    
    def send_batch(self, messages: list) -> Dict:
        """
        Send multiple messages to SQS in batch
        
        Args:
            messages: List of message dictionaries
            
        Returns:
            Response from SQS
        """
        if not self.queue_url:
            raise ValueError("SQS queue URL not configured")
        
        try:
            # Prepare batch entries
            entries = []
            for i, msg in enumerate(messages[:10]):  # SQS limit is 10 per batch
                entry = {
                    'Id': str(i),
                    'MessageBody': json.dumps(msg, ensure_ascii=False)
                }
                
                # Add message attributes
                attributes = {}
                if 'provider' in msg:
                    attributes['Provider'] = {
                        'StringValue': msg['provider'],
                        'DataType': 'String'
                    }
                if 'type' in msg:
                    attributes['FileType'] = {
                        'StringValue': msg['type'],
                        'DataType': 'String'
                    }
                
                if attributes:
                    entry['MessageAttributes'] = attributes
                
                entries.append(entry)
            
            # Send batch
            response = self.sqs_client.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries
            )
            
            # Log results
            if 'Successful' in response:
                logger.info(f"Successfully sent {len(response['Successful'])} messages")
            
            if 'Failed' in response:
                logger.error(f"Failed to send {len(response['Failed'])} messages")
                for failure in response['Failed']:
                    logger.error(f"  Failed: {failure['Id']} - {failure['Message']}")
            
            return response
            
        except ClientError as e:
            logger.error(f"Failed to send batch to SQS: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in batch send: {e}")
            raise
    
    def create_queue(self, queue_name: str, **kwargs) -> str:
        """
        Create an SQS queue (for testing/setup)
        
        Args:
            queue_name: Name of the queue
            **kwargs: Additional queue attributes
            
        Returns:
            Queue URL
        """
        try:
            response = self.sqs_client.create_queue(
                QueueName=queue_name,
                Attributes=kwargs
            )
            queue_url = response['QueueUrl']
            logger.info(f"Created queue: {queue_url}")
            return queue_url
        except ClientError as e:
            if e.response['Error']['Code'] == 'QueueAlreadyExists':
                # Get existing queue URL
                response = self.sqs_client.get_queue_url(QueueName=queue_name)
                return response['QueueUrl']
            else:
                raise
