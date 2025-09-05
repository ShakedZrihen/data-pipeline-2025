import os, io, time, json, gzip
import boto3
from datetime import datetime , timezone
from db_state import LastRunStore
from sqs_producer import SQSProducer
from utils.xml_utils import parse_xml_items, iso_from_filename
from typing import Optional, List

class Extractor:
    def __init__(self):
        self.bucket     = os.getenv("S3_BUCKET", "supermarkets")
        self.endpoint   = os.getenv("LOCALSTACK_ENDPOINT", "http://localstack:4566")
        self.region     = os.getenv("AWS_REGION", "us-east-1")
        self.poll_sec   = int(os.getenv("POLL_SECONDS", "10"))
        self.state_path = os.getenv("STATE_PATH", "./.seen_keys.json")
        self.s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=self.region,
        )

        self.producer = SQSProducer(os.getenv("SQS_QUEUE_URL"))
        self.seen = self._load_seen()
        self.state = LastRunStore(table_name=os.getenv("DDB_TABLE"), region=self.region, endpoint_url=self.endpoint)
        print(f"Extractor ready. Bucket={self.bucket}, seen={len(self.seen)} keys")


    def poll(self):
        print(f"Start polling every {self.poll_sec}s ...")
        while True:
            try:
                keys = self._list_gz_keys()
                new_keys = [k for k in keys if k not in self.seen]
                if not new_keys:
                    print("No new files found")
                    time.sleep(self.poll_sec)
                    continue
                print(f"Found {len(new_keys)} new files")
                for key in new_keys:
                    self._handle_key(key)
                time.sleep(self.poll_sec)
            except Exception as e:
                print("Poll error:", e)
                time.sleep(self.poll_sec)

    def _handle_key(self, key: str):
        print("Processing:", key)
        data = self._build_payload_from_gz(key)
        if not data:
            print("Skip (no data)")
            return
        self.producer.send(data)
        print(f"Sent {data['item_count']} items from {key} to SQS")
        self.state.update(
            data["provider"],
            data["branch"],
            data["type"],
            data["timestamp"],
            data["source_key"],
        )
        print("State updated in DynamoDB for this supermarket" , data["provider"], data["branch"])
        self.seen.add(key)
        self._save_seen()

    def _build_payload_from_gz(self, key: str):
        """
        Download + decompress a .gz XML, parse items, and build a normalized payload.
        Returns a dict ready to JSON-serialize and send to SQS, or None on error/empty.
        """
        # 1) download + streaming decompress to XML text
        try:
            print(f"Downloading and decompressing {key} ...")
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            # Stream directly from the HTTP body to gzip without reading all bytes first
            with gzip.GzipFile(fileobj=obj["Body"]) as gz:
                xml_bytes = gz.read()
            items , store_id = parse_xml_items(xml_bytes)
        except Exception as e:
            print(f"Failed to read/decompress {key}: {e}")
            return None

        # 2) parse XML into normalized items
        if not items:
            print(f"No items parsed from XML: {key}")
            return None

        # 3) provider/branch/type from key
        parts = key.split("/")
        provider = parts[0] if len(parts) > 0 else ""
        branch   = parts[1] if len(parts) > 1 else ""
        fname    = parts[-1].lower() if parts else ""
        # crude type by filename (keep your rule)
        ftype    = "pricesFull" if fname.startswith("price") else "promoFull"

        # 4) timestamp preference: filename → max(item.updated_at) → now
        ts_iso = iso_from_filename(fname)

        # If filename had no timestamp pattern, try max updated_at in items
        if not ts_iso or "T" not in ts_iso:
            def _parse_iso(s: Optional[str]) -> Optional[datetime]:
                if not s:
                    return None
                try:
                    # '...Z' → replace with +00:00 for fromisoformat
                    if s.endswith("Z"):
                        s = s.replace("Z", "+00:00")
                    return datetime.fromisoformat(s)
                except Exception:
                    return None
            max_dt = None
            for it in items:
                dt = _parse_iso(it.get("updated_at"))
                if dt and (max_dt is None or dt > max_dt):
                    max_dt = dt
            if max_dt:
                ts_iso = max_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            else:
                ts_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        # 5) final payload
        data = {
            "provider": provider,
            "branch": branch,
            "store_id": store_id,
            "type": ftype,
            "timestamp": ts_iso,
            "source_key": key,
            "item_count": len(items),
            "items": items,
        }


        return data

    def _list_gz_keys(self):
        keys = []
        token = None
        print("Listing .gz files in bucket ...")
        while True:
            params = {"Bucket": self.bucket}
            if token:
                params["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**params)
            for obj in resp.get("Contents", []):
                k = obj["Key"]
                if k.lower().endswith(".gz"):
                    keys.append(k)
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break
        return keys

    def _load_seen(self):
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                arr = json.load(f)
                return set(arr)
        except:
            return set()

    def _save_seen(self):
        try:
            with open(self.state_path, "w", encoding="utf-8") as f:
                json.dump(sorted(list(self.seen)), f)
        except Exception as e:
            print("Failed to save state:", e)

if __name__ == "__main__":
    Extractor().poll()
