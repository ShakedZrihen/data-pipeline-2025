import gzip, io, json, time, datetime as dt
from pymongo import MongoClient
from shared.config import settings
from shared.s3 import list_objects, get_object_bytes
from shared.mq import publish_json
from .provider_parsers import parse_by_filename, parse_ndjson_gz
import os

S3_PREFIX = os.getenv("S3_PREFIX", "prices/")
STATE_KEY="last_processed_key"

def get_state():
    client = MongoClient(settings.mongo_url)
    db = client.pipeline_state
    state = db.extractor_state.find_one({"_id": STATE_KEY}) or {"_id": STATE_KEY, "value": ""}
    return client, db, state

def set_state(db, value):
    db.extractor_state.update_one({"_id": STATE_KEY}, {"$set": {"value": value}}, upsert=True)

def process_key(key: str):
    data = get_object_bytes(key)
    if not data:
        return 0
    count = 0
    # Try provider-specific parser first
    iterator = parse_by_filename(key, data)
    if iterator is None:
        # Fallback: assume NDJSON.gz
        iterator = parse_ndjson_gz(data)
    for obj in iterator:
        try:
            obj["price"] = float(obj.get("price") or 0)
            publish_json(obj)
            count += 1
        except Exception as e:
            print("Bad record:", e)
    print(f"Pushed {count} messages from {key}")
    return count


if __name__ == "__main__":
    import time
    processed_total = 0
    while True:
        client, db, state = get_state()
        last_key = state.get("value","")
        cycle_processed = 0
        for key in sorted(list(list_objects(prefix=S3_PREFIX))):
            if key <= last_key:
                continue
            cycle_processed += process_key(key)
            set_state(db, key)
            last_key = key
        processed_total += cycle_processed
        if cycle_processed:
            print(f"Extractor cycle processed {cycle_processed} objects (total {processed_total}).")
        else:
            print("Extractor cycle: no new files.")
        time.sleep(60)
