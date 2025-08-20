import boto3
import os
import json
from pathlib import Path

from sqs.sqs_producer import send_message_to_sqs
from utils import extract_and_delete_gz, convert_xml_to_json, sanitize_path_component

from normalizer import normalize_file, chunk_for_sqs

class Extractor:
    def __init__(self):
        print("starting boto3")
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:4566"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        self.bucket = os.getenv("S3_BUCKET", "providers")
        self.work_root = os.getenv("WORK_DIR", "work")
        os.makedirs(self.work_root, exist_ok=True)


    def save_local_normalized(self, normalized, json_src_path, part_idx=None):
            path = Path(json_src_path)
            try:
                work_dir = path.parts.index("work")
                base_dir = Path(*path.parts[:work_dir+3])  # .../work/provider/branch
            except ValueError:
                base_dir = path.parent
                
            out_dir = base_dir / "normalized"
            out_dir.mkdir(parents=True, exist_ok=True)

            suffix = "" if part_idx is None else f".part{part_idx}"
            out_path = out_dir / (path.stem + f".normalized{suffix}.json")
            
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            print(f"Saved normalized JSON: {out_path}")
            return str(out_path)


    def format_converted_json(self, json_path):
        normalized = normalize_file(json_path)
        items_len = len(normalized.get("items", []))
        if items_len == 0:
            print("No items after normalization, skipping SQS send.")

        part = 1
        for chunk in chunk_for_sqs(normalized):
            body = json.dumps(chunk, ensure_ascii=False)
            send_message_to_sqs(body)
            print(f"Message sent to SQS for {json_path} part {part}.")
            multi = (len(chunk.get('items', [])) < items_len)
            self.save_local_normalized(chunk, json_path, part_idx=(part if multi else None))
            part += 1


    def extract_from_s3(self):
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket)
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj["Key"]
                    if not key.lower().endswith(".gz"):
                        continue

                    print(f"Handling: {key}")
                    parts = key.split("/")
                    try:
                        provider, branch, fname = parts[-3], parts[-2], parts[-1]
                    except Exception:
                        provider, branch, fname = "unknown", "unknown", os.path.basename(key)
                    
                    provider = sanitize_path_component(provider)
                    branch = sanitize_path_component(branch)

                    target_dir = os.path.join(self.work_root, provider, branch)
                    os.makedirs(target_dir, exist_ok=True)

                    local_path = os.path.join(target_dir, fname)
                    self.s3.download_file(self.bucket, key, local_path)
                    print(f"saved: {local_path}")
            
                    extracted = extract_and_delete_gz(local_path, True)
                    if not extracted:
                        print("Extraction failed, skipping.")
                        continue

                    converted_json = convert_xml_to_json(extracted)
                    if not converted_json:
                        print("Conversion failed, skipping.")
                        continue

                    print(f"JSON: {converted_json}")
                    self.format_converted_json(converted_json)
            else:
                print("No objects found in the bucket.")
    
        except Exception as e:
                print(f"Error handling GET request: {e}")