import os, json, traceback
import boto3
from botocore.config import Config
from utils.logging_config import setup_logging
from parser import decompress_gz_to_bytes, parse_content, normalize_rows
from normalizer import parse_key
from producer import send_message
from db import upsert_last_run
import tempfile

log = setup_logging()
def _s3_client():
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    region = os.getenv("AWS_REGION", "us-east-1")
    cfg = Config(retries={'max_attempts': 3, 'mode': 'standard'})
    return boto3.client("s3", region_name=region, endpoint_url=endpoint, config=cfg)

def lambda_handler(event, context=None):
    log.info(f"Received event: {json.dumps(event)[:500]}")
    records = event.get("Records", [])
    if not records:
        log.warning("No records in event")
        return {"ok": True, "processed": 0}

    s3 = _s3_client()
    processed = 0
    errors = 0

    for rec in records:
        try:
            send_message(payload)
            bucket = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]
            if not key.endswith(".gz"):
                continue
            provider, branch, type_, iso_ts = parse_key(key)
            obj = s3.get_object(Bucket=bucket, Key=key)
            raw = obj["Body"].read()
            buf = decompress_gz_to_bytes(raw)
            fmt, rows = parse_content(buf)
            items = normalize_rows(rows)

            payload = {
                "provider": provider,
                "branch": branch,
                "type": type_,
                "timestamp": iso_ts,
                "items": items,
            }

            # save locally in Lambda's /tmp
            safe_key = key.replace("/", "__")
            TMP_DIR = os.getenv("LAMBDA_TMP") or os.getenv("TMPDIR") or ("/tmp" if os.name != "nt" else tempfile.gettempdir())
            os.makedirs(TMP_DIR, exist_ok=True)
            out_path = os.path.join(TMP_DIR, f"{safe_key}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            log.info(f"Saved local JSON to {out_path}")

            send_message(payload)
            upsert_last_run(provider, branch, type_, iso_ts)
            processed += 1
        except Exception as e:
            errors += 1
            log.error(f"Error: {e}\n{traceback.format_exc()}")

    return {"ok": errors == 0, "processed": processed, "errors": errors}

def main(event_path="sample_event.json"):
    with open(event_path, "r", encoding="utf-8") as f:
        event = json.load(f)
    resp = lambda_handler(event)
    print(json.dumps(resp, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "sample_event.json"
    main(path)
# === Local simulator run (no AWS) ===
import os, json, tempfile
from pathlib import Path
from config import S3_SIMULATOR_ROOT


def process_local_file(file_path: str):
    """
    מעבד קובץ .gz לוקאלי שנמצא תחת:
      <S3_SIMULATOR_ROOT>/providers/<provider>/<branch>/<filename>.gz
    מייצר payload JSON, שומר ל-outbox, שולח ל-producer, ומסמן בבסיס נתונים/קובץ state.
    """
    p = Path(file_path).resolve()
    root = Path(S3_SIMULATOR_ROOT).resolve() / "providers"
    rel = p.relative_to(root)
    key = f"providers/{str(rel).replace(os.sep, '/')}"

    provider, branch, type_, iso_ts = parse_key(key)

    with open(p, "rb") as f:
        raw = f.read()

    buf = decompress_gz_to_bytes(raw)
    fmt, rows = parse_content(buf)
    items = normalize_rows(rows)

    payload = {
        "provider": provider,
        "branch": branch,
        "type": type_,
        "timestamp": iso_ts,
        "items": items,
    }

    OUTBOX = Path(__file__).parent / "outbox"
    OUTBOX.mkdir(parents=True, exist_ok=True)
    safe_ts = iso_ts.replace(":", "").replace("-", "").replace("T", "_").replace("Z", "")
    out_name = f"{provider}_{branch}_{type_}_{safe_ts}.json"
    out_path = OUTBOX / out_name
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(f"[local] saved: {out_path}")

    send_message(payload)

    upsert_last_run(provider, branch, type_, iso_ts)

    return payload
