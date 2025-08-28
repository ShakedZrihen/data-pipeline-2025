import boto3
import pika
import json
import time
import os
from datetime import datetime
from botocore.config import Config

def create_s3_client():
    """Create S3 client for LocalStack"""
    return boto3.client(
        's3',
        endpoint_url='http://localstack:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1',
        config=Config(signature_version='s3v4')
    )

def create_rabbitmq_connection():
    """Create RabbitMQ connection"""
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='rabbitmq',
                    port=5672,
                    virtual_host='/',
                    credentials=pika.PlainCredentials('guest', 'guest')
                )
            )
            return connection
        except Exception as e:
            retry_count += 1
            print(f"Failed to connect to RabbitMQ (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(2)
    
    raise Exception("Failed to connect to RabbitMQ after maximum retries")

def setup_rabbitmq_queue(channel, queue_name):
    """Setup RabbitMQ queue"""
    channel.queue_declare(queue=queue_name, durable=True)
    print(f"Queue '{queue_name}' declared")

def get_latest_s3_files(s3_client, bucket_name, processed_files):
    """Get list of new files from S3 bucket"""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='prices/')
        
        new_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key not in processed_files:
                    new_files.append(key)
        
        return new_files
    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        return []

def process_s3_file(s3_client, bucket_name, s3_key, channel, queue_name):
    """Download file from S3 and send to RabbitMQ"""
    try:
        # Download file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Parse JSON to validate and extract items
        data = json.loads(file_content)
        root_data = data.get('Root', {})
        items = root_data.get('Items', {}).get('Item', [])
        
        # Extract store information from root
        chain_id = root_data.get('ChainId')
        store_id = root_data.get('StoreId')
        
        if not isinstance(items, list):
            items = [items]
        
        print(f"Processing {len(items)} items from {s3_key} (Store: {store_id}, Chain: {chain_id})")
        
        # Send each item to RabbitMQ
        for item in items:
            message = {
                'source_file': s3_key,
                'timestamp': datetime.now().isoformat(),
                'chain_id': chain_id,
                'store_id': store_id,
                'item_data': item
            }
            
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                )
            )
        
        print(f"Successfully sent {len(items)} messages from {s3_key} to queue '{queue_name}'")
        return True
        
    except Exception as e:
        print(f"Error processing file {s3_key}: {e}")
        return False

def main():
    bucket_name = os.getenv('S3_BUCKET', 'price-data')
    queue_name = os.getenv('RABBITMQ_QUEUE', 'price-items')
    check_interval = int(os.getenv('CHECK_INTERVAL', '30'))
    
    print("Starting S3 to RabbitMQ processor...")
    print(f"S3 Bucket: {bucket_name}")
    print(f"RabbitMQ Queue: {queue_name}")
    print(f"Check interval: {check_interval} seconds")
    
    # Wait for services to be ready
    time.sleep(15)
    
    s3_client = create_s3_client()
    connection = create_rabbitmq_connection()
    channel = connection.channel()
    setup_rabbitmq_queue(channel, queue_name)
    
    processed_files = set()
    
    try:
        while True:
            new_files = get_latest_s3_files(s3_client, bucket_name, processed_files)
            
            for s3_key in new_files:
                print(f"Processing new file: {s3_key}")
                success = process_s3_file(s3_client, bucket_name, s3_key, channel, queue_name)
                if success:
                    processed_files.add(s3_key)
            
            if new_files:
                print(f"Processed {len(new_files)} new files")
            
            time.sleep(check_interval)
            
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        connection.close()

if __name__ == "__main__":
    main()