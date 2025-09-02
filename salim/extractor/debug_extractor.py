import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """Debug version to see what's happening"""
    try:
        print(f"DEBUG: Received event: {json.dumps(event)}")
        
        records = event.get('Records', [])
        print(f"DEBUG: Found {len(records)} records")
        
        for i, record in enumerate(records):
            print(f"DEBUG: Record {i}: {json.dumps(record)}")
            
            s3_info = record.get('s3', {})
            bucket = s3_info.get('bucket', {}).get('name')
            key = s3_info.get('object', {}).get('key')
            
            print(f"DEBUG: Bucket: {bucket}, Key: {key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Debug completed',
                'records_found': len(records)
            })
        }
        
    except Exception as e:
        print(f"DEBUG: Error: {str(e)}")
        import traceback
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }