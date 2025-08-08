"""
Database Handler Module
Tracks last run timestamps for providers
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError
    HAS_BOTO = True
except ImportError:
    HAS_BOTO = False
    logger.warning("boto3 not available, DynamoDB features disabled")

try:
    import pymongo
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False
    logger.warning("pymongo not available, MongoDB features disabled")


class DatabaseHandler:
    """
    Handles database operations for tracking last run times
    Supports both MongoDB (local) and DynamoDB (AWS)
    """
    
    def __init__(self, db_type: str = 'local', **kwargs):
        """
        Initialize database handler
        
        Args:
            db_type: 'mongodb', 'dynamodb', or 'local' (file-based)
            **kwargs: Database-specific configuration
        """
        self.db_type = db_type
        self.db_client = None
        self.table_name = kwargs.get('table_name', 'salim_last_runs')
        
        if db_type == 'dynamodb' and HAS_BOTO:
            self._init_dynamodb(kwargs.get('region', 'il-central-1'))
        elif db_type == 'mongodb' and HAS_PYMONGO:
            self._init_mongodb(
                kwargs.get('connection_string', 'mongodb://localhost:27017/'),
                kwargs.get('database', 'salim')
            )
        else:
            # Fallback to local file storage
            self.db_type = 'local'
            self.local_file = kwargs.get('local_file', '/tmp/last_runs.json')
            logger.info(f"Using local file storage: {self.local_file}")
    
    def _init_dynamodb(self, region: str):
        """Initialize DynamoDB client"""
        try:
            self.db_client = boto3.resource('dynamodb', region_name=region)
            self.table = self.db_client.Table(self.table_name)
            logger.info(f"Connected to DynamoDB table: {self.table_name}")
        except Exception as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            self.db_type = 'local'
    
    def _init_mongodb(self, connection_string: str, database: str):
        """Initialize MongoDB client"""
        try:
            self.db_client = pymongo.MongoClient(connection_string)
            self.db = self.db_client[database]
            self.collection = self.db[self.table_name]
            logger.info(f"Connected to MongoDB: {database}.{self.table_name}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.db_type = 'local'
    
    def update_last_run(self, provider: str, branch: str, 
                       file_type: str, timestamp: Optional[datetime] = None) -> bool:
        """
        Update the last run timestamp for a provider/branch/type combination
        
        Args:
            provider: Provider name (e.g., 'victory')
            branch: Branch name (e.g., 'tel-aviv')
            file_type: File type (e.g., 'pricesFull')
            timestamp: Timestamp to set (defaults to now)
            
        Returns:
            Success status
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        key = f"{provider}_{branch}_{file_type}"
        
        try:
            if self.db_type == 'dynamodb':
                return self._update_dynamodb(key, provider, branch, file_type, timestamp)
            elif self.db_type == 'mongodb':
                return self._update_mongodb(key, provider, branch, file_type, timestamp)
            else:
                return self._update_local(key, provider, branch, file_type, timestamp)
        except Exception as e:
            logger.error(f"Failed to update last run: {e}")
            return False
    
    def _update_dynamodb(self, key: str, provider: str, branch: str, 
                        file_type: str, timestamp: datetime) -> bool:
        """Update DynamoDB record"""
        try:
            self.table.put_item(
                Item={
                    'id': key,
                    'provider': provider,
                    'branch': branch,
                    'file_type': file_type,
                    'last_run': timestamp.isoformat(),
                    'updated_at': datetime.now(timezone.utc).isoformat()
                }
            )
            logger.info(f"Updated DynamoDB last run for {key}")
            return True
        except ClientError as e:
            logger.error(f"DynamoDB update failed: {e}")
            return False
    
    def _update_mongodb(self, key: str, provider: str, branch: str, 
                       file_type: str, timestamp: datetime) -> bool:
        """Update MongoDB record"""
        try:
            self.collection.update_one(
                {'_id': key},
                {
                    '$set': {
                        'provider': provider,
                        'branch': branch,
                        'file_type': file_type,
                        'last_run': timestamp.isoformat(),
                        'updated_at': datetime.now(timezone.utc).isoformat()
                    }
                },
                upsert=True
            )
            logger.info(f"Updated MongoDB last run for {key}")
            return True
        except Exception as e:
            logger.error(f"MongoDB update failed: {e}")
            return False
    
    def _update_local(self, key: str, provider: str, branch: str, 
                     file_type: str, timestamp: datetime) -> bool:
        """Update local file record"""
        try:
            # Load existing data
            data = {}
            if os.path.exists(self.local_file):
                try:
                    with open(self.local_file, 'r') as f:
                        data = json.load(f)
                except:
                    data = {}
            
            # Update record
            data[key] = {
                'provider': provider,
                'branch': branch,
                'file_type': file_type,
                'last_run': timestamp.isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Save back
            os.makedirs(os.path.dirname(self.local_file), exist_ok=True)
            with open(self.local_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Updated local last run for {key} in {self.local_file}")
            return True
        except Exception as e:
            logger.error(f"Local file update failed: {e}")
            return False
    
    def get_last_run(self, provider: str, branch: str, file_type: str) -> Optional[datetime]:
        """
        Get the last run timestamp for a provider/branch/type combination
        
        Args:
            provider: Provider name
            branch: Branch name
            file_type: File type
            
        Returns:
            Last run datetime or None if not found
        """
        key = f"{provider}_{branch}_{file_type}"
        
        try:
            if self.db_type == 'dynamodb':
                response = self.table.get_item(Key={'id': key})
                if 'Item' in response:
                    return datetime.fromisoformat(response['Item']['last_run'])
            elif self.db_type == 'mongodb':
                doc = self.collection.find_one({'_id': key})
                if doc:
                    return datetime.fromisoformat(doc['last_run'])
            else:
                if os.path.exists(self.local_file):
                    with open(self.local_file, 'r') as f:
                        data = json.load(f)
                        if key in data:
                            return datetime.fromisoformat(data[key]['last_run'])
        except Exception as e:
            logger.error(f"Failed to get last run: {e}")
        
        return None
