import os
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import traceback

import boto3
from botocore.client import Config

from sqs.sqs_producer import send_message_to_sqs
from utils import extract_and_delete_gz, convert_xml_to_json, sanitize_path_component
from normalizer import normalize_file, chunk_for_sqs
from last_save_ts import LastRunStore


class Extractor:
    def __init__(self):
        print("starting boto3")

        self.s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:4566")
        self.region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.bucket = os.getenv("S3_BUCKET", "providers")
        self.work_root = os.getenv("WORK_DIR", "work")
        self.delete_raw = os.getenv("DELETE_RAW_AFTER_PROCESS", "1").lower() in ("1", "true", "yes", "y")

        os.makedirs(self.work_root, exist_ok=True)

        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            region_name=self.region,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            config=Config(s3={"addressing_style": "path"}),
        )

        self.store = LastRunStore()

    def save_local_normalized(self, normalized, json_src_path, part_idx=None):
        path = Path(json_src_path)
        try:
            work_dir = path.parts.index("work")
            base_dir = Path(*path.parts[: work_dir + 3])
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
            body_len = len(body.encode("utf-8"))
            print(f"[DEBUG] chunk bytes={body_len} (part {part})")

            send_message_to_sqs(body)
            print(f"Message sent to SQS for {json_path} part {part}.")

            multi = len(chunk.get("items", [])) < items_len
            self.save_local_normalized(chunk, json_path, part_idx=(part if multi else None))
            part += 1

    @staticmethod
    def _provider_branch_from_key(key: str) -> Tuple[str, str]:
        parts = key.split("/")
        try:
            provider, branch = parts[-3], parts[-2]
        except Exception:
            provider, branch = "unknown", "unknown"
        provider = sanitize_path_component(provider)
        branch = sanitize_path_component(branch)
        return provider, branch

    def process_object(self, key: str, last_modified) -> bool:
        """Download → extract → convert → normalize → SQS → cleanup."""
        try:
            print(f"Handling: {key}")
            provider, branch = self._provider_branch_from_key(key)
            fname = os.path.basename(key)

            target_dir = os.path.join(self.work_root, provider, branch)
            os.makedirs(target_dir, exist_ok=True)

            local_path = os.path.join(target_dir, fname)
            self.s3.download_file(self.bucket, key, local_path)
            print(f"saved: {local_path}")

            extracted_xml = extract_and_delete_gz(local_path, delete_gz=True)
            if not extracted_xml:
                print("Extraction failed, skipping.")
                return False

            converted_json = convert_xml_to_json(extracted_xml)
            if not converted_json:
                print("Conversion failed, skipping.")
                return False

            print(f"JSON: {converted_json}")

            self.format_converted_json(converted_json)

            if self.delete_raw:
                try:
                    if os.path.exists(extracted_xml):
                        os.remove(extracted_xml)
                        print(f"[CLEANUP] removed XML: {extracted_xml}")
                except OSError:
                    pass
                try:
                    if os.path.exists(converted_json):
                        os.remove(converted_json)
                        print(f"[CLEANUP] removed JSON: {converted_json}")
                except OSError:
                    pass

            return True

        except Exception as e:
            print(f"[ERROR] processing {key}: {type(e).__name__}: {e}")
            traceback.print_exc()
            return False

    def extract_from_s3(self):
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket)

            groups: Dict[Tuple[str, str], List[dict]] = {}
            total = 0

            for page in pages:
                for obj in page.get("Contents", []):
                    total += 1
                    key = obj.get("Key")
                    if not key or not key.lower().endswith(".gz"):
                        continue
                    provider, branch = self._provider_branch_from_key(key)
                    groups.setdefault((provider, branch), []).append(obj)

            if not groups:
                print("No objects found in the bucket.")
                return

            print(f"[INFO] listed {total} object(s); {sum(len(v) for v in groups.values())} candidate .gz file(s)")

            for (provider, branch), items in groups.items():
                items.sort(key=lambda o: o.get("LastModified"))

                last_ok = self.store.get_last_success(provider, branch, job="extractor")
                if last_ok:
                    items = LastRunStore.filter_s3_objects_since(items, last_ok)

                if not items:
                    print(f"[SKIP] {provider}/{branch}: nothing newer than last_success")
                    continue

                print(f"[INFO] {provider}/{branch}: {len(items)} object(s) to process since {last_ok.isoformat() if last_ok else 'N/A'}")

                latest_seen = None
                had_error = False

                for obj in items:
                    key = obj["Key"]
                    lm = obj.get("LastModified")
                    ok = self.process_object(key, lm)
                    if ok and lm:
                        latest_seen = lm
                    elif not ok:
                        had_error = True
                        self.store.set_failure(provider, branch, error_msg=f"failed {key}", job="extractor")

                # advance cursor only if this (provider,branch) had no errors
                if latest_seen and not had_error:
                    self.store.set_success(provider, branch, when=latest_seen, job="extractor")
                else:
                    if had_error:
                        print(f"[RESULT] not advancing cursor for {provider}/{branch} due to errors")

        except Exception as e:
            print(f"Error handling GET request: {e}")

    def run(self):
        self.extract_from_s3()
