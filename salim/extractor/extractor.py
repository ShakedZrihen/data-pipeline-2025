import boto3
import os
import sys
import gzip
import json
import shutil
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError
from rabbitmq_publisher import RabbitMQPublisher

class Extractor:
    """Extractor that processes files from S3 and sends to RabbitMQ"""

    def __init__(self, endpoint_url: str = 'http://localhost:4566'):
        """Initialize S3 client and RabbitMQ publisher"""
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )
        self.bucket_name = 'test-bucket'
        self.output_dir = 'extracted_files'

        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)

        # Initialize RabbitMQ publisher
        self.rabbitmq = RabbitMQPublisher()

    def list_s3_files(self, prefix: str = "") -> List[Dict[str, Any]]:
        """List all files in S3 bucket with optional prefix"""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)

            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })

            return files

        except Exception as e:
            print(f"Error listing S3 files: {e}")
            return []

    def download_from_s3(self, s3_key: str, local_path: str) -> bool:
        """Download a file from S3"""
        try:
            print(f" Downloading s3://{self.bucket_name}/{s3_key} to {local_path}")
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print(f"Successfully downloaded to {local_path}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                print(f"File not found: s3://{self.bucket_name}/{s3_key}")
            elif error_code == 'NoSuchBucket':
                print(f"Bucket not found: {self.bucket_name}")
            else:
                print(f"Error downloading file: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False

    def extract_gz_to_xml(self, gz_path: str, delete_gz: bool = False) -> Optional[str]:
        """Extract a .gz file to XML format"""
        if not gz_path.endswith(".gz"):
            print(f"Not a .gz file: {gz_path}")
            return None

        # Determine output filename by removing ".gz"
        output_path = gz_path[:-3]

        try:
            with gzip.open(gz_path, "rb") as f_in:
                with open(output_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            print(f"✅ Extracted to: {output_path}")

            # Delete the original .gz file
            if delete_gz:
                os.remove(gz_path)
                print(f"🗑️  Deleted: {gz_path}")

            return output_path

        except Exception as e:
            print(f"Error extracting {gz_path}: {e}")
            return None

    def convert_xml_to_json(self, xml_file_path: str) -> Optional[str]:
        """
        Converts an XML file (even if extensionless) to a JSON file.
        Skips conversion if the JSON file already exists.
        """
        json_file_path = xml_file_path + ".json"
        if os.path.exists(json_file_path):
            print(f"✅ JSON already exists: {json_file_path}")
            return json_file_path

        try:
            # Step 1: Read XML
            with open(xml_file_path, "r", encoding="utf-8") as f:
                xml_data = f.read()

            # Step 2: Parse XML
            root = ET.fromstring(xml_data)

            # Step 3: Convert recursively
            def elem_to_dict(elem):
                result = {elem.tag: {} if elem.attrib else None}
                children = list(elem)
                if children:
                    dd = {}
                    for dc in map(elem_to_dict, children):
                        for k, v in dc.items():
                            if k in dd:
                                if not isinstance(dd[k], list):
                                    dd[k] = [dd[k]]
                                dd[k].append(v)
                            else:
                                dd[k] = v
                    result = {elem.tag: dd}
                if elem.attrib:
                    result[elem.tag].update(("@" + k, v) for k, v in elem.attrib.items())
                if elem.text and elem.text.strip():
                    text = elem.text.strip()
                    if children or elem.attrib:
                        result[elem.tag]["#text"] = text
                    else:
                        result[elem.tag] = text
                return result

            parsed_dict = elem_to_dict(root)

            # Step 4: Save to JSON
            with open(json_file_path, "w", encoding="utf-8") as json_file:
                json.dump(parsed_dict, json_file, ensure_ascii=False, indent=2)

            print(f"✅ Converted to JSON: {json_file_path}")
            return json_file_path
        except Exception as e:
            print(f"❌ Error converting {xml_file_path} to JSON: {e}")
            return None


    def process_local_file(self, gz_path: str, keep_xml: bool = False) -> Optional[str]:
        """Process a local .gz file: extract to XML and convert to JSON"""
        print(f"\n Processing local file: {gz_path}")

        # Step 1: Extract gzip to XML
        xml_path = self.extract_gz_to_xml(gz_path, delete_gz=False)
        if not xml_path:
            return None

        # Step 2: Convert XML to JSON
        json_path = self.convert_xml_to_json(xml_path)
        if not json_path:
            return None

        # Step 3: Clean up files
        if not keep_xml and os.path.exists(xml_path):
            os.remove(xml_path)
            print(f"️  Deleted XML file: {xml_path}")

        # Delete original gz file
        if os.path.exists(gz_path):
            os.remove(gz_path)
            print(f"  Deleted: {gz_path}")

        return json_path

    def process_s3_file(self, s3_key: str, keep_temp_files: bool = False,
                       send_to_rabbitmq: bool = True) -> Optional[str]:
        """Download from S3, process gzip file to JSON, and send to RabbitMQ"""
        print(f"\n Processing S3 file: {s3_key}")

        # Create local file path
        filename = os.path.basename(s3_key)
        local_gz_path = os.path.join(self.output_dir, filename)

        # Step 1: Download from S3
        if not self.download_from_s3(s3_key, local_gz_path):
            return None

        # Step 2: Extract gzip to XML
        xml_path = self.extract_gz_to_xml(local_gz_path, delete_gz=not keep_temp_files)
        if not xml_path:
            return None

        # Step 3: Convert XML to JSON
        json_path = self.convert_xml_to_json(xml_path)
        if not json_path:
            return None

        # Step 4: Send to RabbitMQ
        if send_to_rabbitmq:
            supermarket = s3_key.split('/')[0] if '/' in s3_key else 'unknown'
            self.rabbitmq.publish_file(json_path, supermarket)

        # Step 5: Clean up temporary files
        if not keep_temp_files:
            if os.path.exists(local_gz_path):
                os.remove(local_gz_path)
                print(f"  Deleted downloaded file: {local_gz_path}")
            if os.path.exists(xml_path):
                os.remove(xml_path)
                print(f" Deleted XML file: {xml_path}")

        return json_path



    def extract_all_s3_files(self, prefix: str = "", keep_temp_files: bool = False, 
                            send_to_rabbitmq: bool = True) -> List[str]:
        """Extract all files from S3 and send to RabbitMQ"""
        print(f" Finding files in S3 with prefix: '{prefix}'")

        files = self.list_s3_files(prefix)
        if not files:
            print(f" No files found with prefix: {prefix}")
            return []

        # Filter for .gz files only
        gz_files = [f for f in files if f['key'].endswith('.gz')]

        if not gz_files:
            print(f" No .gz files found with prefix: {prefix}")
            return []

        print(f" Found {len(gz_files)} .gz files to process")

        processed_files = []
        for i, file_info in enumerate(gz_files, 1):
            print(f"\n[{i}/{len(gz_files)}] Processing {file_info['key']}")
            json_path = self.process_s3_file(file_info['key'], keep_temp_files, send_to_rabbitmq)
            if json_path:
                processed_files.append(json_path)

        return processed_files

    def extract_by_supermarket(self, supermarket: str, keep_temp_files: bool = False) -> List[str]:
        """Extract all files for a specific supermarket"""
        print(f" Extracting files for supermarket: {supermarket}")
        return self.extract_all_s3_files(f"{supermarket}/", keep_temp_files)

    def extract_latest_files(self, keep_temp_files: bool = False, send_to_rabbitmq: bool = True) -> List[str]:
        """Extract latest files from each supermarket"""
        print(" Extracting latest files from each supermarket...")

        supermarkets = ['doralon', 'keshet', 'osherad', 'ramilevi', 'tivtaam', 'yohananof']
        processed_files = []

        for supermarket in supermarkets:
            print(f"\n📁 Processing {supermarket}...")

            # Get all files for this supermarket
            files = self.list_s3_files(f"{supermarket}/")
            gz_files = [f for f in files if f['key'].endswith('.gz')]

            if not gz_files:
                print(f"   No files found for {supermarket}")
                continue

            # Group by file type and get latest
            price_files = [f for f in gz_files if 'PriceFull' in f['key']]
            promo_files = [f for f in gz_files if 'PromoFull' in f['key']]

            # Get latest file of each type
            latest_files = []
            if price_files:
                latest_price = max(price_files, key=lambda x: x['last_modified'])
                latest_files.append(latest_price)
                print(f"   Latest PriceFull: {latest_price['key']}")

            if promo_files:
                latest_promo = max(promo_files, key=lambda x: x['last_modified'])
                latest_files.append(latest_promo)
                print(f"   Latest PromoFull: {latest_promo['key']}")

            # Process latest files
            for file_info in latest_files:
                json_path = self.process_s3_file(file_info['key'], keep_temp_files, send_to_rabbitmq)
                if json_path:
                    processed_files.append(json_path)

        return processed_files

    def close(self):
        """Close RabbitMQ connection"""
        if hasattr(self, 'rabbitmq'):
            self.rabbitmq.close()

def main():
    """Main function to handle command line arguments"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Extract and send to RabbitMQ: python extractor.py --latest")
        print("  Extract without RabbitMQ: python extractor.py --latest --no-rabbitmq")
        print("  List files: python extractor.py --list [prefix]")
        sys.exit(1)

    # Initialize extractor
    extractor = Extractor()

    command = sys.argv[1]
    keep_temp = "--keep-temp" in sys.argv
    send_to_rabbitmq = "--no-rabbitmq" not in sys.argv

    try:
        if command == "--list":
            prefix = sys.argv[2] if len(sys.argv) > 2 else ""
            files = extractor.list_s3_files(prefix)

            if files:
                print(f"\n Files in s3://{extractor.bucket_name}/:")
                for file_info in files:
                    print(f"  - {file_info['key']} (Size: {file_info['size']} bytes)")
            else:
                print(f"No files found in s3://{extractor.bucket_name}/")

        elif command == "--latest":
            processed = extractor.extract_latest_files(keep_temp, send_to_rabbitmq)
            print(f"\n Extracted {len(processed)} files")
            if send_to_rabbitmq:
                print(" All files sent to RabbitMQ queues")

        elif command == "--all":
            prefix = sys.argv[2] if len(sys.argv) > 2 else ""
            processed = extractor.extract_all_s3_files(prefix, keep_temp, send_to_rabbitmq)
            print(f"\n Extracted {len(processed)} files")
            if send_to_rabbitmq:
                print(" All files sent to RabbitMQ queues")

        else:
            print(f" Unknown command: {command}")
            sys.exit(1)

    finally:
        extractor.close()

if __name__ == "__main__":
    main()
