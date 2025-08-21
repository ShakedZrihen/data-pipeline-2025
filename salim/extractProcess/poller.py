import time
from .config import S3_BUCKET, STATE_BACKEND
from .s3io import s3, ensure_bucket
from .queue_rabbit import ensure_rabbit
from .state_mongo import ensure_mongo
from .lambda_handle import lambda_handler

def run():
    print("Lambda poller started. Scanning S3 every 10s …")

    try:
        ensure_bucket(S3_BUCKET)
    except Exception as e:
        print(f"[Init] Bucket ensure failed: {e}")

    try:
        ensure_rabbit()
    except Exception as e:
        print(f"[Init] Queue ensure failed: {e}")

    try:
        if STATE_BACKEND == "mongo":
            ensure_mongo()
    except Exception as e:
        print(f"[Init] State ensure failed: {e}")

    seen_keys = set()
    while True:
        try:
            resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="providers/")
            for obj in resp.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".gz") and key not in seen_keys:
                    print(f"New file detected: {key}")
                    event = {
                        "Records": [{
                            "eventName": "ObjectCreated:Put",
                            "s3": {"bucket": {"name": S3_BUCKET}, "object": {"key": key}}
                        }]
                    }
                    lambda_handler(event)
                    seen_keys.add(key)
        except Exception as e:
            print(f"Poll error: {e}")
        time.sleep(10)
