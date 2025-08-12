import json
import os
import urllib.parse

import utils
from extractor import FileExtractor
from mq.producer import RabbitMQProducer


def handler(event, context):
    print("Lambda triggered by S3 event:")

    # Get the first record (there can be multiple if multiple objects triggered at once)
    record = event["Records"][0]

    bucket_name = record["s3"]["bucket"]["name"]
    object_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    print(f"Extracting file from bucket {bucket_name} with key {object_key}")
    extractor = FileExtractor()
    paths = extractor.extract_single_file(object_key)
    json_str = utils.xml_file_to_json(paths[0], as_str=True)
    host = os.environ.get("RABBIT_HOST", "localhost")
    with RabbitMQProducer(host=host) as producer:
        producer.send_queue_message(str(json_str))

    print("File extraction completed")

    return {"statusCode": 200, "body": json.dumps("successfuly extracted file.")}
