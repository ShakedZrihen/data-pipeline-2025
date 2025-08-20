import os, boto3

queue_url = os.environ["SQS_QUEUE_URL"]
endpoint  = os.getenv("AWS_ENDPOINT_URL")
with open("sample_msg.json", "r", encoding="utf-8") as f:
    body = f.read()

sqs = boto3.client("sqs", endpoint_url=endpoint, region_name=os.getenv("AWS_REGION","us-east-1"))
resp = sqs.send_message(QueueUrl=queue_url, MessageBody=body)
print("Sent. MessageId =", resp["MessageId"])
