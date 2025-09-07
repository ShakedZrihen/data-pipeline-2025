import boto3
import gzip
import json
import pika
import os
import xml.etree.ElementTree as ET
from datetime import datetime

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
RABBIT_QUEUE = os.environ.get("RABBIT_QUEUE", "extractor-queue")
TABLE_NAME = "ExtractorLastRun"

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

def parse_xml_gz(raw_data, limit=None):
    """Unzip and parse XML Lines -> JSON list"""
    try:
        xml_text = gzip.decompress(raw_data).decode("utf-8")
    except OSError:
        print("⚠️ Skipping file: not a valid .gz archive (maybe a misnamed .zip)")
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"⚠️ Skipping file: XML parse error: {e}")
        return []

    records = []
    for i, line in enumerate(root.findall(".//Line")):
        record = {child.tag: child.text for child in line}
        records.append(record)
        if limit and (i + 1) >= limit:
            break
    return records

def process_file(bucket, key, limit=5):
    """Process one S3 file: unzip, parse, save timestamp, send to RabbitMQ"""
    if not key.endswith(".gz"):
        print(f"⚠️ Skipping file: {key} (not .gz)")
        return

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw_data = obj["Body"].read()

    records = parse_xml_gz(raw_data, limit=limit)
    if not records:
        print(f"⚠️ No valid records extracted from {key}")
        return

    json_data = {
        "provider": key.split("/")[1],
        "branch": key.split("/")[2],
        "file": os.path.basename(key),
        "timestamp": datetime.utcnow().isoformat(),
        "records": records
    }

    # Save last run
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item={
        "provider_branch_type": f"{json_data['provider']}_{json_data['branch']}_{json_data['file']}",
        "last_run": json_data["timestamp"]
    })

    # Push to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
    channel.basic_publish(exchange="", routing_key=RABBIT_QUEUE, body=json.dumps(json_data))
    connection.close()

    print(f"✅ Processed {key} → RabbitMQ with {len(records)} records")


