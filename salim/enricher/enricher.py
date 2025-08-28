import json
import os
import boto3
from typing import Dict, List, Any
from data_saver import save_enriched_data


QUEUE_NAME = os.getenv("QUEUE_NAME")
DLQ_QUEUE_NAME = os.getenv("DLQ_QUEUE_NAME")
REGION = os.getenv('REGION')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
ENDPOINT_URL = os.getenv('ENDPOINT_URL')


    

sqs = boto3.client(
    'sqs',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)

def init_sqs_queue(queue_name):
    try:
        response = sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                'DelaySeconds': '0',
                'MessageRetentionPeriod': '86400'
            }
        )
        return response['QueueUrl']
    except Exception as e:
        print(f"Failed to create SQS queue: {e}")
        raise

def receive_messages(queue_url: str, max_messages: int = 10) -> List[Dict]:
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=10,
            MessageAttributeNames=['All']
        )
        return response.get('Messages', [])
    except Exception as e:
        print(f"Failed to receive messages: {e}")
        return []

def process_messages(messages: List[Dict], chunks_by_source: Dict[str, Dict[int, Dict[str, Any]]]) -> Dict[str, Any]:

    for message in messages:
        try:
            attrs = message.get('MessageAttributes', {})
            source_key = attrs.get('source_key', {}).get('StringValue')
            chunk_info = attrs.get('chunk_info', {}).get('StringValue', '1/1')
            timestamp = attrs.get('timestamp', {}).get('StringValue')
            
            _, total_chunks = map(int, chunk_info.split('/'))
            
            body = json.loads(message['Body'])
            
            if source_key:
                if source_key not in chunks_by_source:
                    chunks_by_source[source_key] = {
                        'received_chunks': 1,
                        'total_chunks': total_chunks,
                        'data': [body]
                    }
                else:
                    chunks_by_source[source_key]['received_chunks'] += 1
                    chunks_by_source[source_key]['data'].append(body)
                    
                chunks = chunks_by_source[source_key]
                if chunks['received_chunks'] == total_chunks:
                   
                    print(f"Complete message for {chunks['received_chunks']} with {chunks['total_chunks']} chunks received")

        except Exception as e:
            print(f"Error processing message: {e}")
            continue


def delete_messages(queue_url: str, receipt_handles: List[str]) -> None:
    for receipt_handle in receipt_handles:
        try:
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            print(f"Failed to delete message: {e}")

def main():
    try:
        queue_response = sqs.get_queue_url(QueueName=QUEUE_NAME)
        queue_url = queue_response['QueueUrl']
        print(f"Connected to queue: {queue_url}")

        queue_dlq_url = init_sqs_queue(DLQ_QUEUE_NAME)
        print(f"Created and connected to DLQ: {queue_dlq_url}")

        queue_attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        msg_count = int(queue_attrs['Attributes']['ApproximateNumberOfMessages'])
        print(f"Messages in queue: {msg_count}")

        chunks_by_source = {} 
        while True:
            messages = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                VisibilityTimeout=90,
                WaitTimeSeconds=20,
                MessageAttributeNames=['All']
            )
            
            if 'Messages' not in messages:
                print("No messages available. Queue might be empty.")
                break

            print(f"Received {len(messages['Messages'])} messages")
            
            receipt_handles = [msg['ReceiptHandle'] for msg in messages['Messages']]
            delete_messages(queue_url, receipt_handles)
            print(f"Deleted messages with receipt handle, length: {len(receipt_handles)}")
            
            process_messages(messages['Messages'], chunks_by_source)
            
        for source_key, chunks in chunks_by_source.items():
            total_chunks = chunks['total_chunks']
            received_chunks = chunks['received_chunks']
            print(f"\nPartially received {source_key}: {received_chunks}/{total_chunks} chunks")
            
    except Exception as e:
        print(f"Error: {e}")
    
    try:
        if chunks_by_source:
            save_enriched_data(chunks_by_source, queue_dlq_url)
            print("Data saved to database successfully")
    except Exception as e:
        print(f"Failed to save data: {e}")

if __name__ == "__main__":
    main()