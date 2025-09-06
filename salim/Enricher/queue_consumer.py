
import json
import os
import sys
from typing import Any, Dict, List, Optional

import pika
import requests
from dotenv import load_dotenv, find_dotenv

from enricher import enrich_items_from_extractor_json
from io import StringIO

def robust_load_env():
    path = find_dotenv(usecwd=True)
    if not path:
        print("[env] .env not found; relying on OS env")
        return
    try:
        load_dotenv(dotenv_path=path, override=True, encoding="utf-8")
        print(f"[env] loaded .env (utf-8): {path}")
        return
    except UnicodeDecodeError:
        pass
    for enc in ("utf-16", "utf-16-le", "utf-16-be"):
        try:
            with open(path, "r", encoding=enc) as f:
                text = f.read()
            load_dotenv(stream=StringIO(text), override=True)
            print(f"[env] loaded .env ({enc}): {path}")
            return
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f".env at {path} is not UTF-8/UTF-16; re-save the file as UTF-8")
robust_load_env()

SUPABASE_URL =os.getenv("DATABASE_URL").rstrip("/")
if SUPABASE_URL.startswith("postgres://") or SUPABASE_URL.startswith("postgresql://"):
    print("[FATAL] SUPABASE_URL/DATABASE_URL looks like a Postgres connection string.")
    print("        For REST, set SUPABASE_URL='https://<project>.supabase.co'")
    sys.exit(1)

SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "prices")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("[FATAL] Missing SUPABASE_URL (or http DATABASE_URL) or SUPABASE_SERVICE_ROLE_KEY/ANON key.")
    print(f"SUPABASE_URL: {SUPABASE_URL}")
    print(f"SUPABASE_SERVICE_KEY: {'SET' if SUPABASE_SERVICE_KEY else 'NOT SET'}")
    sys.exit(1)

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "salim_queue")

def supabase_upsert_rows(rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print("[upsert] no rows to upsert")
        return
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?on_conflict=store_id,item_code"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    batch = int(os.getenv("BATCH_SIZE", "1000"))
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        resp = requests.post(url, headers=headers, data=json.dumps(chunk))
        if not resp.ok:
            print(f"[upsert] ERROR {resp.status_code}: {resp.text[:500]}")
        else:
            print(f"[upsert] inserted/merged {len(chunk)} rows")
def _wrap_doc_from_message(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize message to the enricher's expected doc shape:
    {
      'original_filename': ...,
      'extraction_timestamp': ...,
      'file_size': ...,
      'last_modified': ... (optional),
      'data': { ... extractor data with Items ... }
    }
    Supported shapes:
      - { 'doc': { ...full doc... } }
      - { 'action': 'process_json_data', 'data': { ... }, ...meta }
      - { 'action': 'process_json_file', 'file_path': '...'}  # will load from disk
    """
    if isinstance(msg.get("doc"), dict) and "data" in msg["doc"]:
        return msg["doc"]

    if "data" in msg and isinstance(msg["data"], dict):
        return {
            "original_filename": msg.get("original_filename"),
            "extraction_timestamp": msg.get("timestamp"),
            "file_size": msg.get("file_size"),
            "last_modified": msg.get("last_modified"),
            "data": msg["data"],
        }
    if msg.get("action") == "process_json_file" and "file_path" in msg:
        try:
            with open(msg["file_path"], "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[warn] failed to load file_path={msg['file_path']}: {e}")
            return None

    return None

def handle_message(msg: Any) -> None:
    if isinstance(msg, list):
        for m in msg:
            if isinstance(m, dict):
                handle_message(m)
        return

    if not isinstance(msg, dict):
        print("[warn] message ignored (not dict/list)")
        return

    doc = _wrap_doc_from_message(msg)
    if not doc:
        print("[warn] unrecognized message shape; skipping")
        return

    rows = enrich_items_from_extractor_json(doc)
    print(f"[enrich] produced {len(rows)} rows")
    supabase_upsert_rows(rows)

def main() -> None:
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    conn = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, credentials=creds
    ))
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)

    def _cb(chx, method, props, body):
        try:
            msg = json.loads(body)
        except Exception as e:
            print(f"[error] bad JSON: {e}")
            chx.basic_ack(delivery_tag=method.delivery_tag)
            return
        try:
            handle_message(msg)
        finally:
            chx.basic_ack(delivery_tag=method.delivery_tag)

    print(f"[consumer] waiting on '{QUEUE_NAME}' at {RABBIT_HOST}:{RABBIT_PORT}")
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=_cb)
    ch.start_consuming()

if __name__ == "__main__":
    main()
