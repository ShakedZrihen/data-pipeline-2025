import json

def send(sqs_client, queue_url: str, payload: dict):
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload, ensure_ascii=False)
    )
