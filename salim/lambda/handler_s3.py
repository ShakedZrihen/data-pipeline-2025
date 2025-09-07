import os, sys, json, subprocess
from urllib.parse import unquote_plus

EXTRACTOR_OUT_DIR = os.getenv("EXTRACTOR_OUT_DIR", "/tmp/extractor_data")
EXTRACTOR_MANIFEST = os.getenv("EXTRACTOR_MANIFEST", "/tmp/manifest/manifest.json")

def run_extractor(bucket: str, key: str):
    script = os.path.join(os.path.dirname(__file__), "extractor", "extractor.py")
    cmd = [
        sys.executable, script,
        "--bucket", bucket,
        "--only-key", key,
    ]
    env = os.environ.copy()
    env["EXTRACTOR_OUT_DIR"] = EXTRACTOR_OUT_DIR
    env["EXTRACTOR_MANIFEST"] = EXTRACTOR_MANIFEST
    subprocess.run(cmd, check=True, env=env)

def lambda_handler(event, context=None):
    records = event.get("Records", [])
    handled = []
    for r in records:
        if r.get("eventSource") != "aws:s3":
            continue
        bucket = r["s3"]["bucket"]["name"]
        key = unquote_plus(r["s3"]["object"]["key"])
        if not key.endswith(".gz"):
            continue
        run_extractor(bucket, key)
        handled.append({"bucket": bucket, "key": key})
    return {"statusCode": 200, "body": json.dumps({"handled": handled}, ensure_ascii=False)}