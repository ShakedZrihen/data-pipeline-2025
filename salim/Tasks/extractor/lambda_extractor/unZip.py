
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import config
from xml_normalizer import parse_gz_xml_bytes, build_payload
from s3_writer import write_payload_json
from sqs_writer import sqs_writer


class UnZip:
    @staticmethod
    def _parse_timestamp_from_filename(filename: str):
        """
        Extract timestamp from filename patterns like:
        pricesFull_20250818211435.gz -> 2025-08-18T21:14:35Z
        """
        try:
            # split on underscore and take last numeric part
            ts_part = filename.split("_")[-1].replace(".gz", "")
            dt = datetime.strptime(ts_part, "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            # fallback: now
            return datetime.now(timezone.utc)


    @staticmethod
    def _read_and_unzip(s3, bucket_name, key):
        """Download, unzip, parse XML, normalize into dict."""
        try:
            print(f"Reading {key} from bucket {bucket_name}...")
            response = s3.get_object(Bucket=bucket_name, Key=key)
            gz_bytes = response["Body"].read()
            print(f"Downloaded {len(gz_bytes)} bytes from {key}")
            # parse XML root
            root = parse_gz_xml_bytes(gz_bytes)
            print("here")
            # figure out provider + branch from S3 key: providers/<provider>/<branch>/filename
            parts = key.split("/")
            provider = parts[1] if len(parts) > 1 else ""
            branch   = parts[2] if len(parts) > 2 else ""

            print("still here")
            # figure out file type from filename
            filename = parts[-1]
            if "pricesFull" in filename:
                file_type = "pricesFull"
            elif "promoFull" in filename:
                file_type = "promoFull"
            else:
                file_type = "unknown"

            # timestamp → from filename
            ts = UnZip._parse_timestamp_from_filename(filename)

            # build normalized payload
            payload = build_payload(file_type, root, provider, branch, ts)
            print(f"Extracted payload: {payload}")
            return payload

        except ClientError as e:
            print(f"Error reading {key}: {e}")
            return None


    @staticmethod
    def process_prefix(bucket_name: str, prefix: str):
        """
        Process all .gz files under a given 'folder' (prefix) in the bucket.
        """
        s3 = boto3.client(
            "s3",
            endpoint_url=config.ENDPOINT_URL,  # LocalStack or AWS
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=config.AWS_REGION,
        )

        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".gz"):
                        print(f"Processing {key}...")
                        payload = UnZip._read_and_unzip(s3, bucket_name, key)
                        if payload:
                            json_key = write_payload_json(payload)
                            print("Normalized payload sample:" + key)
                            print(f"Wrote normalized JSON to s3://{bucket_name}/{json_key}")

                            # Send SQS message with pointer to the JSON
                            writer = sqs_writer()
                            writer.send_pointer(bucket_name, json_key)
                        else:
                            print(f"Failed to process {key}, skipping.")
        except ClientError as e:
            print(f"Error listing objects in {prefix}: {e}")

if __name__ == "__main__":
    bucket_name = config.S3_BUCKET
    prefix = "providers/"  # Adjust as needed
    UnZip.process_prefix(bucket_name, prefix)
