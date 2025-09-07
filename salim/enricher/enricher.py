import os
import json
import time
import pika
import psycopg2
import psycopg2.extras

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")

QUEUE = os.getenv("QUEUE", "enricher.records")

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "enricher")
PGUSER = os.getenv("PGUSER", "enricher")
PGPASSWORD = os.getenv("PGPASSWORD", "enricher")


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS enriched_stub (
                id SERIAL PRIMARY KEY,
                message JSONB,
                note TEXT,
                created_at TIMESTAMPTZ DEFAULT now()
            );
        """)
        conn.commit()


def upsert_stub(conn, message):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO enriched_stub (message, note) VALUES (%s, %s);",
            (json.dumps(message), "This is a stub enrichment"),
        )
        conn.commit()


def main():
    print("Starting Enricher Stub...")

    # Connect to Postgres
    pg_conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST,
        port=PGPORT,
    )
    ensure_table(pg_conn)

    # Connect to RabbitMQ
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host=RABBIT_VHOST,
        credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASS),
        heartbeat=30,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)

    print("Stub enricher connected. Waiting for messages...")

    while True:
        method, properties, body = ch.basic_get(queue=QUEUE, auto_ack=True)
        if method:
            try:
                msg = json.loads(body.decode("utf-8"))
            except Exception:
                msg = {"raw": body.decode("utf-8")}
            print(f"[STUB] Received message: {msg}")
            upsert_stub(pg_conn, msg)
        else:
            time.sleep(2)  # idle wait if queue is empty


if __name__ == "__main__":
    main()
