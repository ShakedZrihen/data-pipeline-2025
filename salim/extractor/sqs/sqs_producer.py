import boto3
import sys
from botocore.exceptions import ClientError

def send_message_to_sqs(message_body):
    """Send a message to SQS queue using LocalStack"""
    
    print(f"Sending message to SQS queue: {message_body}")
    
    sqs_client = boto3.client(
        'sqs',
        endpoint_url='http://localhost:4567',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    queue_name = 'test-queue'
    
    try:
        # Get queue URL
        queue_url_response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = queue_url_response['QueueUrl']
        print(f"Queue URL: {queue_url}")
        
        # Send message
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )
        
        print(f"✅ Message sent successfully!")
        print(f"   Message ID: {response['MessageId']}")
        if "MD5OfMessageBody" in response:
            print(f"   MD5 of Body: {response['MD5OfMessageBody']}")
        
        # Get queue attributes to show message count
        queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        message_count = queue_attrs['Attributes']['ApproximateNumberOfMessages']
        print(f"   Messages in queue: {message_count}")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
            print(f"Error: Queue '{queue_name}' does not exist!")
            print("Make sure LocalStack services are running with: docker-compose up")
        else:
            print(f"Error sending message: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

def receive_messages_from_sqs():
    """Receive messages from SQS queue"""
    
    print("Receiving messages from SQS queue...")
    
    sqs_client = boto3.client(
        'sqs',
        endpoint_url='http://localhost:4567',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'
    )
    
    queue_name = 'test-queue'
    
    try:
        # Get queue URL
        queue_url_response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = queue_url_response['QueueUrl']
        
        # Receive messages
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1
        )
        
        if 'Messages' in response:
            print(f"📥 Received {len(response['Messages'])} messages:")
            for i, message in enumerate(response['Messages'], 1):
                print(f"   Message {i}:")
                print(f"     ID: {message['MessageId']}")
                print(f"     Body: {message['Body']}")
                print(f"     Receipt Handle: {message['ReceiptHandle'][:20]}...")
                print()
        else:
            print("📭 No messages available in queue")
            
    except ClientError as e:
        print(f"Error receiving messages: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)