import pika
import json
import time

class QueueProducer:
    def __init__(self):
        self.rabbitmq_host = 'localhost'
        self.rabbitmq_port = 5672
        self.rabbitmq_user = 'admin'
        self.rabbitmq_pass = 'admin'
        self.queue_name = 'salim_queue'
        self.connection = None
        self.channel = None
        
    def setup_rabbitmq(self):
        try:
            credentials = pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_pass)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    port=self.rabbitmq_port,
                    credentials=credentials
                )
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            print(f"RabbitMQ queue '{self.queue_name}' is ready")
            
            return True
        except Exception as e:
            print(f"Failed to setup RabbitMQ: {e}")
            return False
  
    def send_to_queue(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  
                )
            )
            print(f"Sent to queue: {message.get('original_filename', 'Unknown file')}")
            return True
        except Exception as e:
            print(f"Failed to send to queue: {e}")
            return False
    
    def send_file_message(self, file_path, original_filename, file_size):
        """Send a file processing message to the queue"""
        queue_message = {
            'action': 'process_json_file',
            'file_path': file_path,
            'original_filename': original_filename,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'file_size': file_size
        }
        return self.send_to_queue(queue_message)
    
    def close_connection(self):
        """Close RabbitMQ connection"""
        if self.connection:
            self.connection.close()
            print("RabbitMQ connection closed")

