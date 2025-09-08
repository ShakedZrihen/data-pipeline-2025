
import json, os, time, traceback
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db_handler import DB
from lambda_extractor import config
from Enricher import Enricher

from typing import Dict, Any, List
import boto3
from botocore.exceptions import BotoCoreError, ClientError


def _stream_read_utf8(body, chunk_bytes: int = 1024 * 1024) -> str:
    """Read a botocore StreamingBody in chunks and decode as UTF-8."""
    parts: List[str] = []
    while True:
        chunk = body.read(chunk_bytes)
        if not chunk:
            break
        parts.append(chunk.decode("utf-8"))
    return "".join(parts)


class SqsS3Consumer:
    def __init__(
        self,
        visibility_timeout: int = int(os.getenv("VISIBILITY_TIMEOUT", "600")),  # ↑ from 60 → 600
        wait_time_seconds: int = 20,
        batch_size: int = 10,
    ):
        self.db = DB()  # your DB wrapper
        self.queue_url = config.QUEUE_URL
        self.region_name = config.AWS_REGION

        # Endpoints: host uses localhost; lambda in container might use 'localstack'
        self.s3 = boto3.client(
            "s3",
            endpoint_url=(config.ENDPOINT_URL or "http://localstack:4566"),
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=self.region_name,
        )
        self.sqs = boto3.client(
            "sqs",
            endpoint_url=(os.getenv("ENDPOINT_URL", "http://localstack:4566")),
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=self.region_name,
        )
        self.visibility_timeout = visibility_timeout
        self.wait_time_seconds = wait_time_seconds
        self.batch_size = max(1, min(batch_size, 10))

        # Debug controls
        self.progress_every = int(os.getenv("PROGRESS_EVERY", "20"))   # log every N items
        self.items_debug_max = int(os.getenv("ITEMS_DEBUG_MAX", "0"))   # 0 = unlimited


    def process_message(self, body: dict) -> None:
        """
        body example (always a pointer from SQS):
        {
          "bucket": "providers-bucket",
          "key": "Json/SuperSapir/46/pricesFull_20250824212658.json"
        }
        """
        bucket = body["bucket"]
        key = body["key"]
        print(f"[S3 POINTER] Fetching s3://{bucket}/{key}")

        # 1) Read JSON file (streamed, to avoid long blocking reads)
        try:
            obj = self.s3.get_object(Bucket=bucket, Key=key)
            content = _stream_read_utf8(obj["Body"])
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                print(f"[MISSING] s3://{bucket}/{key} does not exist → skipping")
                return
            raise

        try:
            payload = json.loads(content)
        except Exception as e:
            print(f"[ERR ] File {key} did not contain valid JSON: {e}")
            return

        # 2) Extract fields from JSON
        e = Enricher()
        provider = str(payload.get("provider") or "").strip()
        branch_code = str(payload.get("branch") or "").strip()
        e_dict = e.enrich(provider, branch_code)
        print(f"[INFO] Enrichment result: {e_dict}")
        if e_dict is not None:
            branch_name = str(e_dict.get("StoreName") or "").strip()
            city = str(e_dict.get("City") or "").strip()
            address = str(e_dict.get("Address") or "").strip()
            print(f"[INFO] Enriched branch data: name={branch_name}, city={city}, address={address}")
        else:
            branch_name = ""
            city = ""
            address = ""
        file_type = str(payload.get("type") or "").strip()
        ts_str = str(payload.get("timestamp") or "").strip()
        print(f"[Info] ts={ts_str})")
        items = payload.get("items") or []

        if not provider or not branch_code or not file_type or not ts_str:
            print(f"[ERR ] Missing required fields in payload. got={list(payload.keys())}")
            return

        price_type = "regular" if file_type == "pricesFull" else "promo" if file_type == "promoFull" else None
        if price_type is None:
            print(f"[ERR ] Unknown feed type: {file_type}")
            return

        effective_at = self.db.parse_feed_timestamp(ts_str)
        supermarket_id = self.db.ensure_supermarket(provider, ts_str)
        branch_id = self.db.ensure_branch(supermarket_id, branch_code,branch_name, city, address, ts_str)

        inserted = 0
        skipped = 0

        total_items = len(items)
        limit = self.items_debug_max if self.items_debug_max > 0 else total_items

        for idx, it in enumerate(items[:limit], start=1):
            try:
                barcode = (it.get("barcode") or "").strip()
                name = (it.get("product") or "").strip()
                unit = it.get("unit") or None
                price_val = it.get("price")

                if not name or price_val is None:
                    skipped += 1
                    continue

                provider_product_id = self.db.upsert_provider_product(
                    supermarket_id=supermarket_id,
                    barcode=barcode,
                    name=name,
                    unit_of_measure=unit,
                    brand=None,
                    quantity=None,
                    created_at=effective_at,
                )
                self.db.insert_or_update_current_price(
                    provider_product_id=provider_product_id,
                    branch_id=branch_id,
                    price_type=price_type,
                    price=float(price_val),
                    effective_at=effective_at,
                    source_file_type=file_type,
                )
                inserted += 1

            except Exception as e:
                skipped += 1
                print(f"[WARN] skipped item #{idx} ({e}): {it!r}")

            # Progress logs for big files
            if idx % self.progress_every == 0:
                print(f"[..] {idx}/{total_items} processed so far (ins={inserted}, skip={skipped})")

        print(
            f"[OK] provider={provider} branch={branch_code} type={file_type} "
            f"ts={effective_at.isoformat()} items={total_items} upserted={inserted} skipped={skipped}"
        )


    def _receive_batch(self) -> List[Dict[str, Any]]:
        try:
            resp = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.batch_size,
                WaitTimeSeconds=self.wait_time_seconds,   # long poll
                VisibilityTimeout=self.visibility_timeout # time to process before it reappears
            )
            return resp.get("Messages", []) or []
        except (ClientError, BotoCoreError) as e:
            print(f"[recv] {e}")
            time.sleep(1)
            return []


    def _delete(self, receipt_handle: str) -> None:
        try:
            self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        except (ClientError, BotoCoreError) as e:
            print(f"[del ] {e}")


    def run_forever(self) -> None:
        print(f"[run ] polling {self.queue_url} …")
        while True:
            msgs = self._receive_batch()
            if not msgs:
                continue
            for m in msgs:
                try:
                    body = json.loads(m["Body"])
                    self.process_message(body)
                    # Always delete when processed/skipped gracefully
                    self._delete(m["ReceiptHandle"])
                except Exception:
                    print("[ERR ] processing failed; leaving message for retry")
                    traceback.print_exc()
            time.sleep(0.1)


if __name__ == "__main__":
    SqsS3Consumer().run_forever()
