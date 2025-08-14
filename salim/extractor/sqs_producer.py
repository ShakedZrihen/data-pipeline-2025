import json
from datetime import datetime, timezone 

UTC = timezone.utc

def send_to_sqs(sqs, queue_url, key, json_data):
    try:
        data = json.loads(json_data)
        max_items = 100
        
        base_attributes = {
            'source_key': {
                'DataType': 'String',
                'StringValue': key
            },
            'timestamp': {
                'DataType': 'String',
                'StringValue': datetime.now(UTC).isoformat()
            }
        }

        if isinstance(data, list):
            total_chunks = (len(data) + max_items - 1) // max_items
            
            for chunk_num in range(total_chunks):
                start_idx = chunk_num * max_items
                end_idx = min((chunk_num + 1) * max_items, len(data))
                chunk_data = data[start_idx:end_idx]
                
                chunk_attributes = base_attributes.copy()
                chunk_attributes.update({
                    'chunk_info': {
                        'DataType': 'String',
                        'StringValue': f"{chunk_num + 1}/{total_chunks}"
                    }
                })
                
                chunk_json = json.dumps(chunk_data, ensure_ascii=False)
                
                response = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=chunk_json,
                    MessageAttributes=chunk_attributes
                )
                print(f"Sent chunk {chunk_num + 1}/{total_chunks} to SQS: {response['MessageId']}")
        else:
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json_data,
                MessageAttributes=base_attributes
            )
            print(f"Sent message to SQS: {response['MessageId']}")
            
    except Exception as sqs_error:
        print(f"Failed to send message to SQS: {sqs_error}")
        raise