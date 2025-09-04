import logging
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from typing import Dict, Any, Optional
from datetime import datetime

from config import config

logger = logging.getLogger(__name__)


class DynamoDBTracker:
    """Handle DynamoDB operations for tracking last run times"""
    
    def __init__(self):
        """Initialize DynamoDB client and table"""
        try:
            self.dynamodb = boto3.resource('dynamodb', region_name=config.aws_region)
            self.table_name = config.dynamodb_table_name
            self.table = self.dynamodb.Table(self.table_name)
            logger.info(f"DynamoDB Tracker initialized with table: {self.table_name}")
        except Exception as e:
            logger.error(f"Failed to initialize DynamoDB client: {str(e)}")
            self.dynamodb = None
            self.table = None
    
    def get_last_run(self, provider: str, branch: str, file_type: str) -> Optional[datetime]:
        """
        Get last run time for provider+branch+type
        
        Args:
            provider: Provider name
            branch: Branch name
            file_type: File type (pricesFull/promoFull)
            
        Returns:
            Last run datetime or None if not found
        """
        if not self.table:
            logger.error("DynamoDB table not properly initialized")
            return None
        
        try:
            # Create composite key
            primary_key = self._create_primary_key(provider, branch, file_type)
            
            response = self.table.get_item(
                Key={'id': primary_key}
            )
            
            item = response.get('Item')
            if item and 'last_run' in item:
                # Parse ISO timestamp back to datetime
                return datetime.fromisoformat(item['last_run'].replace('Z', '+00:00'))
            
            logger.info(f"No last run found for {provider}/{branch}/{file_type}")
            return None
            
        except ClientError as e:
            logger.error(f"DynamoDB ClientError getting last run: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error getting last run: {str(e)}")
            return None
    
    def update_last_run(self, provider: str, branch: str, file_type: str, timestamp: datetime) -> bool:
        """
        Update last run time for provider+branch+type
        
        Args:
            provider: Provider name
            branch: Branch name
            file_type: File type
            timestamp: Run timestamp
            
        Returns:
            True if successful, False otherwise
        """
        if not self.table:
            logger.error("DynamoDB table not properly initialized")
            return False
        
        try:
            # Create composite key
            primary_key = self._create_primary_key(provider, branch, file_type)
            
            # Create item to store
            item = {
                'id': primary_key,
                'provider': provider,
                'branch': branch,
                'file_type': file_type,
                'last_run': timestamp.isoformat() + 'Z',
                'updated_at': datetime.utcnow().isoformat() + 'Z'
            }
            
            # Put item in DynamoDB
            self.table.put_item(Item=item)
            
            logger.info(f"Successfully updated last run for {provider}/{branch}/{file_type} to {timestamp}")
            return True
            
        except ClientError as e:
            logger.error(f"DynamoDB ClientError updating last run: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error updating last run: {str(e)}")
            return False
    
    def record_processing_stats(self, provider: str, branch: str, file_type: str, 
                               items_processed: int, processing_time_ms: int, 
                               success: bool) -> bool:
        """
        Record processing statistics
        
        Args:
            provider: Provider name
            branch: Branch name
            file_type: File type
            items_processed: Number of items processed
            processing_time_ms: Processing time in milliseconds
            success: Whether processing was successful
            
        Returns:
            True if successful, False otherwise
        """
        if not self.table:
            logger.error("DynamoDB table not properly initialized")
            return False
        
        try:
            # Create stats key (separate from last_run tracking)
            stats_key = f"stats#{self._create_primary_key(provider, branch, file_type)}"
            
            # Get current timestamp
            now = datetime.utcnow()
            
            # Create stats item
            stats_item = {
                'id': stats_key,
                'provider': provider,
                'branch': branch,
                'file_type': file_type,
                'items_processed': items_processed,
                'processing_time_ms': processing_time_ms,
                'success': success,
                'timestamp': now.isoformat() + 'Z',
                'date': now.strftime('%Y-%m-%d')  # For daily aggregations
            }
            
            # Store stats
            self.table.put_item(Item=stats_item)
            
            logger.info(f"Recorded processing stats for {provider}/{branch}/{file_type}: "
                       f"{items_processed} items, {processing_time_ms}ms, success={success}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording processing stats: {str(e)}")
            return False
    
    def _create_primary_key(self, provider: str, branch: str, file_type: str) -> str:
        """
        Create composite primary key from provider, branch, and file type
        
        Args:
            provider: Provider name
            branch: Branch name
            file_type: File type
            
        Returns:
            Composite key string
        """
        # Clean and normalize the key components
        clean_provider = provider.replace('#', '-').replace('/', '-').strip()
        clean_branch = branch.replace('#', '-').replace('/', '-').strip()
        clean_type = file_type.replace('#', '-').replace('/', '-').strip()
        
        return f"{clean_provider}#{clean_branch}#{clean_type}"