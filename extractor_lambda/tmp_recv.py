import os, json, boto3
sqs = boto3.client("sqs",
    endpoint_url=os.environ["AWS_ENDPOINT_URL"],
    region_name=os.environ.get("AWS_REGION","us-east-1"))
url = os.environ["OUTPUT_QUEUE_URL"]
resp = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
body = resp["Messages"][0]["Body"]
with open("sqs_msg_utf8.json","w", encoding="utf-8") as f:
    f.write(body)
data = json.loads(body)
print(data["items_sample"][:3])
