import pika
import json
import time

class QueueHandler:
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
    
    def send_file_message(self, file_info):
        """Send a file processing message to the queue"""
        queue_message = {
            'action': 'process_json_file',
            'file_path': file_info['json_file'],
            'original_filename': file_info['original_key'],
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'file_size': file_info['size']
        }
        return self.send_to_queue(queue_message)
    
    def read_from_queue(self):
        """Read a single message from the queue"""
        method_frame, _, body = self.channel.basic_get(queue=self.queue_name, auto_ack=True)
        if method_frame:
            message = json.loads(body)
            print(f"Received from queue: {message.get('original_filename', 'Unknown file')}")
            return message
        else:
            print("No messages in queue")
            return None 
        
    def listen_to_queue(self, callback):
        """Continuously listen to the queue and process messages with the given callback"""
        def on_message(channel, method, properties, body):
            message = json.loads(body)
            print(f"Processing message: {message.get('original_filename', 'Unknown file')}")
            callback(message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=on_message)
        print("Listening to RabbitMQ queue...")
        self.channel.start_consuming()
        
    def list_messages(self):
        """List all messages in the queue without removing them"""
        try:
            queue = self.channel.queue_declare(queue=self.queue_name, passive=True)
            message_count = queue.method.message_count
            print(f"Total messages in queue '{self.queue_name}': {message_count}")
            return message_count
        except Exception as e:
            print(f"Failed to list messages: {e}")
            return 0
        
    def delete_queue(self):
        """Delete the RabbitMQ queue"""
        if self.channel:
            self.channel.queue_delete(queue=self.queue_name)
            print(f"Deleted RabbitMQ queue '{self.queue_name}'")

    def close_connection(self):
        """Close RabbitMQ connection"""
        if self.connection:
            self.connection.close()
            print("RabbitMQ connection closed")

    

