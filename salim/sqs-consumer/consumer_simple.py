import json
import traceback
from datetime import datetime
from typing import Dict, Any, List
import requests
import boto3
from openai_enricher import OpenAIEnricher

class SimpleSQSConsumer:
    """Lambda function to consume SQS messages and save to Supabase database"""
    
    def __init__(self):
        print("[INFO] Starting Simple SQS Consumer")
        
        # Get Supabase credentials from environment
        import os
        self.supabase_url = os.environ.get('SUPABASE_URL')
        self.supabase_key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
        self.dlq_url = os.environ.get('DLQ_QUEUE_URL')
        
        # Initialize OpenAI enricher
        openai_api_key = os.environ.get('OPENAI_API_KEY')
        self.enricher = OpenAIEnricher(openai_api_key) if openai_api_key else None
        
        if not openai_api_key:
            print("[WARNING] OPENAI_API_KEY not found, enrichment will be skipped")
        else:
            print("[INFO] OpenAI enricher initialized successfully")
        
        if not self.supabase_url or not self.supabase_key:
            raise Exception("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables")
        
        # Initialize SQS client for DLQ
        self.sqs_client = boto3.client('sqs') if self.dlq_url else None
        
        # Test connection
        if not self._test_connection():
            raise Exception("Failed to connect to Supabase")
        
        print("[INFO] Simple SQS Consumer initialized successfully")
    
    def _test_connection(self) -> bool:
        """Test Supabase connection"""
        try:
            url = f"{self.supabase_url}/rest/v1/supermarket_data?select=id&limit=1"
            headers = {
                'apikey': self.supabase_key,
                'Authorization': f'Bearer {self.supabase_key}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            print(f"[INFO] Connection test status: {response.status_code}")
            return response.status_code in [200, 404]  # 404 is OK if table is empty
            
        except Exception as e:
            print(f"[ERROR] Connection test failed: {e}")
            return False
    
    def validate_message_schema(self, message_data: Dict[str, Any]) -> tuple[bool, str]:
        """Validate message against expected schema"""
        try:
            # Required fields
            required_fields = ['provider', 'branch', 'type', 'timestamp', 'items']
            
            for field in required_fields:
                if field not in message_data:
                    return False, f"Missing required field: {field}"
                
                if not message_data[field]:
                    return False, f"Empty required field: {field}"
            
            # Validate file type
            valid_types = ['pricesFull', 'promoFull']
            if message_data['type'] not in valid_types:
                return False, f"Invalid file type: {message_data['type']}. Must be one of {valid_types}"
            
            # Validate items structure
            if not isinstance(message_data['items'], list):
                return False, "Items must be a list"
            
            if len(message_data['items']) == 0:
                return False, "Items list cannot be empty"
            
            # Validate each item has required fields
            for i, item in enumerate(message_data['items']):
                if not isinstance(item, dict):
                    return False, f"Item {i} must be a dictionary"
                
                # Check for required item fields (extractor uses 'product' and 'price')
                if 'product' not in item or not item['product']:
                    return False, f"Item {i} missing required field: product"
                if 'price' not in item or item['price'] is None:
                    return False, f"Item {i} missing required field: price"
            
            return True, "Valid"
            
        except Exception as e:
            return False, f"Schema validation error: {str(e)}"
    
    def send_to_dlq(self, record: Dict[str, Any], reason: str) -> bool:
        """Send invalid message to Dead Letter Queue"""
        try:
            if not self.sqs_client or not self.dlq_url:
                print("[WARNING] DLQ not configured, cannot send invalid message")
                return False
            
            # Truncate large messages for DLQ
            original_body = record.get('body', '')
            if len(original_body) > 200000:  # 200KB limit
                original_body = original_body[:200000] + "... [TRUNCATED]"
            
            dlq_message = {
                'original_message': original_body,
                'rejection_reason': reason,
                'rejected_at': datetime.utcnow().isoformat(),
                'message_id': record.get('messageId', 'unknown')
            }
            
            self.sqs_client.send_message(
                QueueUrl=self.dlq_url,
                MessageBody=json.dumps(dlq_message)
            )
            
            print(f"[INFO] Sent invalid message to DLQ: {reason}")
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to send message to DLQ: {e}")
            return False
    
    def handler(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Main Lambda handler for processing SQS messages"""
        try:
            print(f"[INFO] Processing {len(event.get('Records', []))} SQS messages")
            
            results = []
            total_items_processed = 0
            
            for record in event.get('Records', []):
                try:
                    result = self.process_sqs_message(record)
                    results.append(result)
                    
                    if result['status'] == 'success':
                        total_items_processed += result.get('items_processed', 0)
                        print(f"[SUCCESS] Successfully processed message {result['message_id']}: {result['items_processed']} items")
                    else:
                        print(f"[ERROR] Failed to process message {result['message_id']}: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    print(f"[ERROR] Failed to process SQS record: {str(e)}")
                    print(f"[ERROR] {traceback.format_exc()}")
                    results.append({
                        'status': 'error',
                        'error': str(e),
                        'message_id': record.get('messageId', 'unknown')
                    })
            
            successful_count = len([r for r in results if r.get('status') == 'success'])
            error_count = len([r for r in results if r.get('status') == 'error'])
            
            print(f"[INFO] Processing complete: {successful_count} successful, {error_count} errors, {total_items_processed} total items processed")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'SQS messages processed successfully',
                    'processed_messages': successful_count,
                    'failed_messages': error_count,
                    'total_items_processed': total_items_processed,
                    'results': results
                })
            }
            
        except Exception as e:
            print(f"[ERROR] Lambda handler failed: {str(e)}")
            print(f"[ERROR] {traceback.format_exc()}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': str(e),
                    'message': 'Lambda execution failed'
                })
            }
    
    def process_sqs_message(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single SQS message and save to Supabase"""
        message_id = record.get('messageId', 'unknown')
        
        try:
            print(f"[INFO] Processing message ID: {message_id}")
            
            # Parse message body
            message_body = record.get('body', '{}')
            message_data = json.loads(message_body)
            
            # Validate message schema
            is_valid, validation_error = self.validate_message_schema(message_data)
            if not is_valid:
                print(f"[ERROR] Invalid message schema: {validation_error}")
                self.send_to_dlq(record, validation_error)
                return {
                    'status': 'rejected',
                    'message_id': message_id,
                    'reason': validation_error
                }
            
            # Log batch information if available
            batch_info = message_data.get('batch_info')
            if batch_info:
                print(f"[INFO] Processing batch {batch_info.get('batch_number', 'unknown')} with {batch_info.get('total_items_in_batch', 'unknown')} items (original file had {batch_info.get('original_total_items', 'unknown')} items)")
            
            # Extract items from message
            items = self._extract_items_from_message(message_data)
            
            if not items:
                print(f"[WARNING] No items found in message {message_id}")
                return {
                    'status': 'success',
                    'message_id': message_id,
                    'items_processed': 0,
                    'note': 'No items to process'
                }
            
            # Prepare data for Supabase
            processed_data = []
            for item in items:
                processed_item = self._prepare_item_for_db(item, message_id, len(items))
                processed_data.append(processed_item)
            
            # Enrich data with OpenAI
            if self.enricher:
                print(f"[INFO] Enriching {len(processed_data)} items with OpenAI")
                try:
                    processed_data = self.enricher.enrich_items(processed_data)
                    print("[INFO] OpenAI enrichment completed successfully")
                except Exception as e:
                    print(f"[WARNING] OpenAI enrichment failed: {e}")
                    print("[INFO] Continuing without enrichment")
            else:
                print("[INFO] OpenAI enrichment skipped (no API key)")
            
            # Deduplicate data based on unique constraint
            processed_data = self._deduplicate_items(processed_data)
            
            # Save to Supabase
            if self._insert_to_supabase(processed_data):
                print(f"[SUCCESS] Successfully saved {len(processed_data)} items to database for message {message_id}")
                
                # Create summary
                summary = self._create_summary(message_data, items)
                print(f"[INFO] Processing summary - {summary}")
                
                return {
                    'status': 'success',
                    'message_id': message_id,
                    'items_processed': len(items),
                    'provider': message_data.get('provider', 'unknown'),
                    'branch': message_data.get('branch', 'unknown'),
                    'type': message_data.get('type', 'unknown'),
                    'summary': summary
                }
            else:
                raise Exception("Failed to insert data into Supabase")
                
        except Exception as e:
            print(f"[ERROR] Error processing message {message_id}: {str(e)}")
            return {
                'status': 'error',
                'message_id': message_id,
                'error': str(e)
            }
    
    def _extract_items_from_message(self, message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract supermarket items from message data"""
        # Handle different message formats
        if 'items' in message_data:
            items = message_data['items']
            # Add metadata to each item
            for item in items:
                item['provider'] = message_data.get('provider', '')
                item['branch'] = message_data.get('branch', '')
                item['type'] = message_data.get('type')
                item['timestamp'] = message_data.get('timestamp', '')
            return items
        elif 'data' in message_data:
            return message_data['data']
        elif isinstance(message_data, list):
            return message_data
        elif 'ItemName' in message_data or 'product' in message_data:
            return [message_data]
        else:
            print(f"[WARNING] Unknown message format: {list(message_data.keys())}")
            return []
    
    def _prepare_item_for_db(self, item: Dict[str, Any], message_id: str, total_items: int) -> Dict[str, Any]:
        """Prepare item for database insertion"""
        # Convert price to float
        price = self._safe_float(item.get('ItemPrice', item.get('price', 0)))
        
        # Normalize unit
        unit = self._normalize_unit(item.get('UnitOfMeasure', item.get('unit', 'unit')))
        
        return {
            'provider': item.get('ChainName', item.get('provider', '')),
            'branch': item.get('SubChainName', item.get('branch', '')),
            'file_type': item.get('file_type') or item.get('type') or 'unknown',
            'file_timestamp': item.get('PriceUpdateDate', item.get('timestamp', '')),
            'product_name': item.get('ItemName', item.get('product', '')),
            'manufacturer': item.get('ManufacturerName', item.get('manufacturer', '')),
            'price': price,
            'unit': unit,
            'is_promotion': (item.get('file_type') or item.get('type')) == 'promoFull',
            'is_kosher': item.get('is_kosher'),  # Will be enriched by OpenAI
            'category': item.get('category'),    # Will be enriched by OpenAI
            'message_id': message_id,  # Required field for database
            'total_items_in_file': total_items,  # Required field for database
            'processed_at': datetime.utcnow().isoformat(),
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat()
        }
    
    def _safe_float(self, value) -> float:
        """Convert value to float safely"""
        try:
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                cleaned = value.replace('₪', '').replace('$', '').replace(',', '').strip()
                return float(cleaned)
            else:
                return 0.0
        except (ValueError, TypeError):
            return 0.0
    
    def _normalize_unit(self, unit_str: str) -> str:
        """Normalize unit of measure"""
        if not unit_str:
            return 'unit'
        
        unit_lower = unit_str.lower().strip()
        unit_mapping = {
            'kg': 'kilogram', 'kilogram': 'kilogram', 'kilo': 'kilogram',
            'g': 'gram', 'gram': 'gram', 'gr': 'gram',
            'l': 'liter', 'liter': 'liter', 'litre': 'liter',
            'ml': 'milliliter', 'milliliter': 'milliliter',
            'package': 'package', 'bottle': 'bottle', 'bag': 'bag',
            'יח': 'unit', 'יחידה': 'unit', 'unit': 'unit'
        }
        return unit_mapping.get(unit_lower, 'unit')
    
    def _deduplicate_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate items based on unique constraint key"""
        seen_keys = set()
        deduplicated = []
        duplicates_removed = 0
        
        for item in items:
            # Create unique key based on database constraint
            unique_key = (
                item.get('provider', ''),
                item.get('branch', ''),
                item.get('file_type', ''),
                item.get('file_timestamp', ''),
                item.get('product_name', '')
            )
            
            if unique_key not in seen_keys:
                seen_keys.add(unique_key)
                deduplicated.append(item)
            else:
                duplicates_removed += 1
        
        if duplicates_removed > 0:
            print(f"[INFO] Removed {duplicates_removed} duplicate items within the batch")
        
        return deduplicated
    
    def _insert_to_supabase(self, data: List[Dict[str, Any]]) -> bool:
        """Insert data to Supabase using REST API with upsert functionality"""
        try:
            # Now that data is deduplicated, try proper upsert with on_conflict
            print(f"[INFO] Attempting upsert for {len(data)} deduplicated items")
            url = f"{self.supabase_url}/rest/v1/supermarket_data"
            headers = {
                'apikey': self.supabase_key,
                'Authorization': f'Bearer {self.supabase_key}',
                'Content-Type': 'application/json',
                'Prefer': 'resolution=merge-duplicates,return=minimal'
            }
            
            # Add on_conflict parameter to URL for upsert
            upsert_url = f"{url}?on_conflict=provider,branch,file_type,file_timestamp,product_name"
            response = requests.post(upsert_url, json=data, headers=headers, timeout=30)
            
            print(f"[DEBUG] Upsert response: Status={response.status_code}")
            
            if response.status_code in [200, 201]:
                print(f"[SUCCESS] Upsert completed for {len(data)} items")
                return True
            elif response.status_code == 409:
                # This shouldn't happen with deduplicated data, but handle anyway
                print(f"[INFO] Some duplicate entries still exist - treating as success")
                return True
            else:
                print(f"[ERROR] Supabase upsert failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Failed to insert to Supabase: {e}")
            return False
    
    
    def _handle_duplicates(self, data: List[Dict[str, Any]]) -> bool:
        """Handle duplicate entries by updating existing records"""
        try:
            success_count = 0
            duplicate_count = 0
            
            for item in data:
                # Try to update existing record
                url = f"{self.supabase_url}/rest/v1/supermarket_data"
                headers = {
                    'apikey': self.supabase_key,
                    'Authorization': f'Bearer {self.supabase_key}',
                    'Content-Type': 'application/json',
                    'Prefer': 'resolution=merge-duplicates'
                }
                
                # Filter criteria for finding existing record
                filter_params = {
                    'provider': f"eq.{item['provider']}",
                    'branch': f"eq.{item['branch']}",
                    'file_type': f"eq.{item['file_type']}",
                    'file_timestamp': f"eq.{item['file_timestamp']}",
                    'product_name': f"eq.{item['product_name']}"
                }
                
                # Build query string
                query_params = '&'.join([f"{k}={v}" for k, v in filter_params.items()])
                update_url = f"{url}?{query_params}"
                
                # Update the record
                update_data = {
                    'price': item['price'],
                    'updated_at': datetime.utcnow().isoformat(),
                    'processed_at': item['processed_at']
                }
                
                response = requests.patch(update_url, json=update_data, headers=headers, timeout=30)
                
                if response.status_code in [200, 201, 204]:
                    duplicate_count += 1
                else:
                    # If update fails, try insert again (maybe it was a race condition)
                    response = requests.post(url, json=[item], headers=headers, timeout=30)
                    if response.status_code in [200, 201]:
                        success_count += 1
                    elif response.status_code == 409:
                        duplicate_count += 1
                        print(f"[INFO] Duplicate ignored for product: {item.get('product_name', 'unknown')}")
                    else:
                        print(f"[WARNING] Failed to handle duplicate for product: {item.get('product_name', 'unknown')}")
            
            total_handled = success_count + duplicate_count
            print(f"[INFO] Handled duplicates: {success_count} new inserts, {duplicate_count} duplicates updated/ignored out of {len(data)} items")
            
            # Consider it successful if we handled most items
            return total_handled >= len(data) * 0.8
            
        except Exception as e:
            print(f"[ERROR] Failed to handle duplicates: {e}")
            return False
    
    def _create_summary(self, message_data: Dict[str, Any], items: List[Dict[str, Any]]) -> str:
        """Create processing summary"""
        provider = message_data.get('provider', 'Unknown')
        branch = message_data.get('branch', 'Unknown')
        file_type = message_data.get('type', 'Unknown')
        items_count = len(items)
        
        if items:
            prices = [self._safe_float(item.get('ItemPrice', item.get('price', 0))) for item in items]
            valid_prices = [p for p in prices if p > 0]
            
            if valid_prices:
                avg_price = sum(valid_prices) / len(valid_prices)
                min_price = min(valid_prices)
                max_price = max(valid_prices)
                return f"Provider: {provider}, Branch: {branch}, Type: {file_type}, Items: {items_count}, Price Range: {min_price:.2f}-{max_price:.2f} (avg: {avg_price:.2f})"
        
        return f"Provider: {provider}, Branch: {branch}, Type: {file_type}, Items: {items_count}"


# Lambda entry point
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda entry point"""
    consumer = SimpleSQSConsumer()
    return consumer.handler(event, context)