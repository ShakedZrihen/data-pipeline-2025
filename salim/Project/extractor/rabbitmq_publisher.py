import pika
import json
import datetime
import time
from typing import Dict, Any

class RabbitMQPublisher:
    """Publishes data to RabbitMQ queues"""

    def __init__(self, host='localhost', port=5672, username='admin', password='admin'):
        """Initialize RabbitMQ publisher with connection parameters"""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.connection = None
        self.channel = None
        
        # Queue names
        self.queues = {
            'pricefull': 'pricefull_queue',
            'promofull': 'promofull_queue',
            'all': 'all_data_queue'
        }

        # Try to connect with retries
        self.connect_with_retry()
        if self.connection:
            self.setup_queues()

    def connect_with_retry(self, max_retries=30, retry_delay=2):
        """Connect to RabbitMQ with retry logic"""
        for attempt in range(max_retries):
            try:
                print(f"Attempting to connect to RabbitMQ (attempt {attempt + 1}/{max_retries})...")
                self.connect()
                print("Successfully connected to RabbitMQ!")
                return
            except Exception as e:
                print(f"Error: Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                else:
                    print("Error: Failed to connect to RabbitMQ after maximum retries")
                    raise

    def connect(self):
        """Connect to RabbitMQ"""
        try:
            # Connection parameters
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                connection_attempts=3,
                retry_delay=1
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            print(f"Connected to RabbitMQ at {self.host}:{self.port}")

        except Exception as e:
            print(f"Error: Failed to connect to RabbitMQ: {e}")
            raise

    def setup_queues(self):
        """Declare queues"""
        try:
            for queue_name in self.queues.values():
                self.channel.queue_declare(queue=queue_name, durable=True)
                print(f"Queue declared: {queue_name}")

        except Exception as e:
            print(f"Error: Failed to setup queues: {e}")
            raise

    def publish_json(self, data: Dict[str, Any], queue_type: str = 'all', 
                    supermarket: str = None, file_type: str = None):
        """Publish JSON data to RabbitMQ queue"""
        try:
            if not self.connection or self.connection.is_closed:
                print("Connection lost, attempting to reconnect...")
                self.connect_with_retry()

            # Determine queue name
            if queue_type == 'pricefull':
                queue_name = self.queues['pricefull']
            elif queue_type == 'promofull':
                queue_name = self.queues['promofull']
            else:
                queue_name = self.queues['all']

            # Add metadata to the message
            message = {
                'data': data,
                'metadata': {
                    'supermarket': supermarket,
                    'file_type': file_type,
                    'timestamp': str(datetime.datetime.now())
                }
            }

            # Convert to JSON string
            message_body = json.dumps(message, ensure_ascii=False)

            # Publish message
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )

            print(f"Published to {queue_name}: {supermarket} - {file_type}")

        except Exception as e:
            print(f"Error: Failed to publish message: {e}")
            # Try to reconnect and retry once
            try:
                print("Attempting to reconnect and retry...")
                self.connect_with_retry()
                self.publish_json(data, queue_type, supermarket, file_type)
            except Exception as retry_e:
                print(f"Error: Retry failed: {retry_e}")

    def publish_file(self, json_file_path: str, supermarket: str = None):
        """Publish JSON file content to RabbitMQ"""
        try:
            # Read JSON file
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Determine file type
            if 'PriceFull' in json_file_path:
                file_type = 'PriceFull'
                queue_type = 'pricefull'
            elif 'PromoFull' in json_file_path:
                file_type = 'PromoFull'
                queue_type = 'promofull'
            else:
                file_type = 'Unknown'
                queue_type = 'all'

            # Publish to RabbitMQ
            self.publish_json(data, queue_type, supermarket, file_type)

        except Exception as e:
            print(f"Error: Failed to publish file {json_file_path}: {e}")

    def close(self):
        """Close RabbitMQ connection"""
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                print("RabbitMQ connection closed")
        except Exception as e:
            print(f"Error: Error closing RabbitMQ connection: {e}")

    def is_connected(self):
        """Check if connection is alive"""
        return self.connection and not self.connection.is_closed

def main():
    """Test RabbitMQ publisher"""
    publisher = RabbitMQPublisher()

    # Test message
    test_data = {
        "test": "message",
        "timestamp": "2025-01-07"
    }

    publisher.publish_json(test_data, 'all', 'test', 'test')
    publisher.close()

if __name__ == "__main__":
    main()
