import pika
import json
import time
import os
import sys
from typing import Any, Dict, List, Optional
import requests
from dotenv import load_dotenv, find_dotenv
from io import StringIO

class QueueHandler:
    def __init__(self):
        load_dotenv(find_dotenv(usecwd=True))
        # RabbitMQ configuration
        self.rabbitmq_host = os.getenv("RABBIT_HOST", "localhost")
        self.rabbitmq_port = int(os.getenv("RABBIT_PORT", "5672"))
        self.rabbitmq_user = os.getenv("RABBIT_USER", "admin")
        self.rabbitmq_pass = os.getenv("RABBIT_PASS", "admin")
        self.queue_name = os.getenv("QUEUE_NAME", "salim_queue")
        
        # Supabase configuration
        self.supabase_url = os.getenv("DATABASE_URL", "").rstrip("/")
        if self.supabase_url.startswith("postgres://") or self.supabase_url.startswith("postgresql://"):
            print("[FATAL] SUPABASE_URL/DATABASE_URL looks like a Postgres connection string.")
            print("        For REST, set SUPABASE_URL='https://<project>.supabase.co'")
            sys.exit(1)
        
        self.supabase_service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
        self.supabase_table = os.getenv("SUPABASE_TABLE", "prices")
        
        if not self.supabase_url or not self.supabase_service_key:
            print("[FATAL] Missing SUPABASE_URL (or http DATABASE_URL) or SUPABASE_SERVICE_ROLE_KEY/ANON key.")
            print(f"SUPABASE_URL: {self.supabase_url}")
            print(f"SUPABASE_SERVICE_KEY: {'SET' if self.supabase_service_key else 'NOT SET'}")
            sys.exit(1)
        
        self.connection = None
        self.channel = None

    def setup_rabbitmq(self):
        try:
            credentials = pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_pass)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    port=self.rabbitmq_port,
                    credentials=credentials
                )
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            print(f"RabbitMQ queue '{self.queue_name}' is ready")
            
            return True
        except Exception as e:
            print(f"Failed to setup RabbitMQ: {e}")
            return False
  
    def send_to_queue(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  
                )
            )
            print(f"Sent to queue: {message.get('original_filename', 'Unknown file')}")
            return True
        except Exception as e:
            print(f"Failed to send to queue: {e}")
            return False

    def read_from_queue(self):
        """Read a single message from the queue"""
        method_frame, _, body = self.channel.basic_get(queue=self.queue_name, auto_ack=True)
        if method_frame:
            message = json.loads(body)
            print(f"Received from queue: {message.get('original_filename', 'Unknown file')}")
            return message
        else:
            print("No messages in queue")
            return None 
        
    def listen_to_queue(self, callback):
        """Continuously listen to the queue and process messages with the given callback"""
        def on_message(channel, method, properties, body):
            message = json.loads(body)
            print(f"Processing message: {message.get('original_filename', 'Unknown file')}")
            callback(message)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=on_message)
        print("Listening to RabbitMQ queue...")
        self.channel.start_consuming()
        
    def list_messages(self):
        """List all messages in the queue without removing them"""
        try:
            queue = self.channel.queue_declare(queue=self.queue_name, passive=True)
            message_count = queue.method.message_count
            print(f"Total messages in queue '{self.queue_name}': {message_count}")
            return message_count
        except Exception as e:
            print(f"Failed to list messages: {e}")
            return 0
        
    def delete_queue(self):
        """Delete the RabbitMQ queue"""
        if self.channel:
            self.channel.queue_delete(queue=self.queue_name)
            print(f"Deleted RabbitMQ queue '{self.queue_name}'")

    def close_connection(self):
        """Close RabbitMQ connection"""
        if self.connection:
            self.connection.close()
            print("RabbitMQ connection closed")

    # Consumer functionality methods below
    
    def supabase_upsert_rows(self, rows: List[Dict[str, Any]]) -> None:
        """Upsert rows to Supabase table"""
        if not rows:
            print("[upsert] no rows to upsert")
            return
        url = f"{self.supabase_url}/rest/v1/{self.supabase_table}?on_conflict=store_id,item_code"
        headers = {
            "apikey": self.supabase_service_key,
            "Authorization": f"Bearer {self.supabase_service_key}",
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

    def _wrap_doc_from_message(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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

    def handle_message(self, msg: Any, callback) -> None:
        """Handle incoming messages from the queue"""
        if isinstance(msg, list):
            for m in msg:
                if isinstance(m, dict):
                    self.handle_message(m, callback)
            return

        if not isinstance(msg, dict):
            print("[warn] message ignored (not dict/list)")
            return

        doc = self._wrap_doc_from_message(msg)
        if not doc:
            print("[warn] unrecognized message shape; skipping")
            return

        # Import enricher here to avoid circular imports
        try:
            rows = callback(doc)
            print(f"[enrich] produced {len(rows)} rows")
            self.supabase_upsert_rows(rows)
        except ImportError as e:
            print(f"[error] Could not import enricher: {e}")
            print("[warn] Message processing skipped due to missing enricher")