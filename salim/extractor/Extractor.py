import os, io, time, json, gzip
import boto3
from datetime import datetime
from sqs_producer import SQSProducer
from utils.xml_utils import parse_xml_items, iso_from_filename

class Extractor:
    def __init__(self):
        self.bucket     = os.getenv("S3_BUCKET", "supermarkets")
        self.endpoint   = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
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
        print(f"Extractor ready. Bucket={self.bucket}, seen={len(self.seen)} keys")


    def poll(self):
        print(f"Start polling every {self.poll_sec}s ...")
        while True:
            try:
                keys = self._list_gz_keys()
                new_keys = [k for k in keys if k not in self.seen]
                if new_keys:
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
        self.seen.add(key)
        self._save_seen()

    # FIXME:  need to implement XML parsing for .gz files
    def _build_payload_from_gz(self, key: str):
        # 1) download + decompress to XML text
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            raw = obj["Body"].read()
            text = gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode("utf-8", errors="ignore")
        except Exception as e:
            print("Failed to read/decompress:", e)
            return None

        # 2) parse XML into items (product, price, unit)
        items = parse_xml_items(text)
        if not items:
            print("No items parsed from XML")
            return None

        # (optional) keep messages small – cap to first 200 rows if you want
        # items = items[:200]

        # 3) pull provider/branch/type from key like: GoodPharm/145/price_20250831_132400.gz
        parts = key.split("/")
        provider = parts[0] if len(parts) > 0 else ""
        branch   = parts[1] if len(parts) > 1 else ""
        fname    = parts[-1].lower() if parts else ""
        ftype    = "pricesFull" if fname.startswith("price") else "promoFull"
        ts_iso   = iso_from_filename(fname)

        # 4) final payload
        data = {
            "provider": provider,
            "branch": branch,
            "type": ftype,
            "timestamp": ts_iso,
            "items": items,
        }
        # quick preview
        print("Built JSON preview:", data["items"][0] if data["items"] else None)
        return data

    def _list_gz_keys(self):
        keys = []
        token = None
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
