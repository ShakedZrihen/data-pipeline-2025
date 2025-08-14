import logging
import boto3
from typing import Dict, Any, Optional
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class DynamoDBHandler:
    
    def __init__(self, table_name: str, region_name: str = 'us-east-1'):
        self.table_name = table_name
        self.region_name = region_name
        self.dynamodb_client = None
        self.dynamodb_resource = None
        
    def _get_clients(self):
        if self.dynamodb_client is None:
            try:
                self.dynamodb_client = boto3.client('dynamodb', region_name=self.region_name)
                self.dynamodb_resource = boto3.resource('dynamodb', region_name=self.region_name)
                logger.info(f"Created DynamoDB clients for region: {self.region_name}")
            except Exception as e:
                logger.error(f"Failed to create DynamoDB clients: {e}")
                raise
        return self.dynamodb_client, self.dynamodb_resource
    
    def create_table_if_not_exists(self):
        try:
            client, resource = self._get_clients()
            
            # Check if table exists
            try:
                client.describe_table(TableName=self.table_name)
                logger.info(f"Table {self.table_name} already exists")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    # Table doesn't exist, create it
                    pass
                else:
                    raise
            
            # Create table
            table = resource.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'provider_branch_type',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'provider_branch_type',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            
            # Wait for table to be created
            table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
            logger.info(f"Successfully created table: {self.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table {self.table_name}: {e}")
            return False
    
    def update_last_run_time(self, provider: str, branch: str, file_type: str, timestamp: str) -> bool:
        """Update the last run time for a specific provider+branch+type combination"""
        try:
            client, resource = self._get_clients()
            
            # Create composite key
            composite_key = f"{provider}_{branch}_{file_type}"
            
            # Use PutItem instead of UpdateItem to avoid reserved word issues
            response = client.put_item(
                TableName=self.table_name,
                Item={
                    'provider_branch_type': {'S': composite_key},
                    'provider': {'S': provider},
                    'branch': {'S': branch},
                    'file_type': {'S': file_type},
                    'last_run': {'S': timestamp},
                    'updated_at': {'S': datetime.utcnow().isoformat()}
                }
            )
            
            logger.info(f"Updated last run time for {composite_key}: {timestamp}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update last run time for {provider}_{branch}_{file_type}: {e}")
            return False
    
    def get_last_run_time(self, provider: str, branch: str, file_type: str) -> Optional[str]:
        """Get the last run time for a specific provider+branch+type combination"""
        try:
            client, resource = self._get_clients()
            
            # Create composite key
            composite_key = f"{provider}_{branch}_{file_type}"
            
            # Get item
            response = client.get_item(
                TableName=self.table_name,
                Key={
                    'provider_branch_type': {'S': composite_key}
                }
            )
            
            if 'Item' in response:
                last_run_time = response['Item'].get('last_run', {}).get('S')
                logger.info(f"Retrieved last run time for {composite_key}: {last_run_time}")
                return last_run_time
            else:
                logger.info(f"No last run time found for {composite_key}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get last run time for {provider}_{branch}_{file_type}: {e}")
            return None
    
    def get_all_last_run_times(self) -> Dict[str, Any]:
        """Get all last run times from the table"""
        try:
            client, resource = self._get_clients()
            
            # Scan table
            response = client.scan(TableName=self.table_name)
            items = response.get('Items', [])
            
            # Process items
            result = {}
            for item in items:
                composite_key = item['provider_branch_type']['S']
                result[composite_key] = {
                    'provider': item.get('provider', {}).get('S', ''),
                    'branch': item.get('branch', {}).get('S', ''),
                    'file_type': item.get('file_type', {}).get('S', ''),
                    'last_run': item.get('last_run', {}).get('S', ''),
                    'updated_at': item.get('updated_at', {}).get('S', '')
                }
            
            logger.info(f"Retrieved {len(result)} last run time records")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get all last run times: {e}")
            return {}
    
    def delete_last_run_time(self, provider: str, branch: str, file_type: str) -> bool:
        """Delete the last run time for a specific provider+branch+type combination"""
        try:
            client, resource = self._get_clients()
            
            # Create composite key
            composite_key = f"{provider}_{branch}_{file_type}"
            
            # Delete item
            response = client.delete_item(
                TableName=self.table_name,
                Key={
                    'provider_branch_type': {'S': composite_key}
                }
            )
            
            logger.info(f"Deleted last run time for {composite_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete last run time for {provider}_{branch}_{file_type}: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test if we can connect to DynamoDB"""
        try:
            client, resource = self._get_clients()
            client.list_tables()
            logger.info("DynamoDB connection test successful")
            return True
        except Exception as e:
            logger.error(f"DynamoDB connection test failed: {e}")
            return False
