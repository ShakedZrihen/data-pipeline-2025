
import json
import os
import sys
from glob import glob
from typing import Iterable, List, Dict, Any

import pika
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# RabbitMQ
RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "salim_queue")

# Defaults
DEFAULT_DIR = "extracted_files"
CHUNK_SIZE = 0  # 0 disables chunking

def _connect():
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT, credentials=creds)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.confirm_delivery()
    return conn, ch

def _iter_json_files(paths: List[str]) -> Iterable[str]:
    if not paths:
        paths = [DEFAULT_DIR]
    for p in paths:
        if os.path.isdir(p):
            for fp in glob(os.path.join(p, "*.json")):
                if os.path.basename(fp).lower().startswith("extraction_summary"):
                    continue
                yield fp
        else:
            if p.lower().endswith(".json"):
                yield p

def _load_doc(fp: str) -> Dict[str, Any]:
    with open(fp, "r", encoding="utf-8") as f:
        return json.load(f)

def _split_doc(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    if CHUNK_SIZE <= 0:
        return [doc]
    items = (((doc or {}).get("data") or {}).get("Items") or {}).get("Item")
    if not isinstance(items, list) or len(items) <= CHUNK_SIZE:
        return [doc]
    parts: List[Dict[str, Any]] = []
    for i in range(0, len(items), CHUNK_SIZE):
        chunk_items = items[i:i+CHUNK_SIZE]
        part = json.loads(json.dumps(doc))  
        part["data"]["Items"]["Item"] = chunk_items
        parts.append(part)
    return parts

def publish_from_files(paths: List[str]) -> None:
    conn, ch = _connect()
    sent = 0
    try:
        for fp in _iter_json_files(paths):
            try:
                doc = _load_doc(fp)
            except Exception as e:
                print(f"[producer] skip {fp}: {e}")
                continue
            for part in _split_doc(doc):
                body = json.dumps({
                    "action": "process_json_data",
                    "doc": part,
                    "original_filename": part.get("original_filename") or os.path.basename(fp),
                    "timestamp": part.get("extraction_timestamp"),
                    "file_size": part.get("file_size") or os.path.getsize(fp),
                }, ensure_ascii=False).encode("utf-8")

                ok = ch.basic_publish(
                    exchange="",
                    routing_key=QUEUE_NAME,
                    body=body,
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2 
                    ),
                    mandatory=False,
                )
                if ok:
                    sent += 1
                    print(f"[producer] sent -> {fp}")
                else:
                    print(f"[producer] publish NOT confirmed for {fp}")
    finally:
        try:
            ch.close()
        except Exception:
            pass
        conn.close()
    print(f"[producer] done. messages sent: {sent}")

if __name__ == "__main__":
    publish_from_files(sys.argv[1:])