import os, json, time, re, zlib
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from sqlalchemy import create_engine, text
from prometheus_client import Counter, start_http_server

AWS_REGION   = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_ENDPOINT = os.getenv("SQS_ENDPOINT", "http://localstack:4566")
IN_QUEUE     = os.getenv("IN_QUEUE", "price-events")
OUT_QUEUE    = os.getenv("OUT_QUEUE", "price-enriched")
DLQ_QUEUE    = os.getenv("DLQ_QUEUE", "price-enricher-dlq")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/salim_db")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9110"))
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", "10"))
WAIT_SECONDS = int(os.getenv("WAIT_SECONDS", "10"))

sqs = boto3.client(
    "sqs",
    endpoint_url=SQS_ENDPOINT,
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID","test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY","test"),
)
engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)

m_ok  = Counter("enricher_msgs_ok_total", "Messages enriched")
m_err = Counter("enricher_msgs_err_total", "Messages failed")

BRANDS = [
    ("תנובה", re.compile(r"\bתנובה\b", re.I)),
    ("שטראוס", re.compile(r"\bשטראוס\b", re.I)),
    ("Coca-Cola", re.compile(r"\bcoca[\s\-]?cola\b", re.I)),
    ("Heinz", re.compile(r"\bheinz\b", re.I)),
]

CATEGORIES = [
    ("Dairy", re.compile(r"\bmilk\b|\bחלב\b", re.I)),
    ("Coffee", re.compile(r"\bcoffee\b|\bקפה\b", re.I)),
    ("Bread", re.compile(r"\bbread\b|\bלחם\b", re.I)),
    ("Sweets", re.compile(r"\bchoco|\bשוקולד\b", re.I)),
]

RE_SIZES = [
    ("L",   re.compile(r"(\d+(?:[\.,]\d+)?)\s*(?:l|liter|ליטר)\b", re.I)),
    ("ml",  re.compile(r"(\d+(?:[\.,]\d+)?)\s*(?:ml|מ\"ל|מיליליטר)\b", re.I)),
    ("kg",  re.compile(r"(\d+(?:[\.,]\d+)?)\s*(?:kg|ק\"ג|קג)\b", re.I)),
    ("g",   re.compile(r"(\d+(?:[\.,]\d+)?)\s*(?:g|gram|גרם)\b", re.I)),
]

def get_queue_url(name: str) -> str:
    try:
        return sqs.get_queue_url(QueueName=name)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        sqs.create_queue(QueueName=name)
        return sqs.get_queue_url(QueueName=name)["QueueUrl"]

def pick_brand(name: str) -> str | None:
    for b, rx in BRANDS:
        if rx.search(name): return b
    return None

def pick_category(name: str) -> str | None:
    for cat, rx in CATEGORIES:
        if rx.search(name): return cat
    return None

def parse_size(name: str, unit: str | None) -> tuple[float | None, str | None]:
    text = f"{name} {unit or ''}"
    for u, rx in RE_SIZES:
        m = rx.search(text)
        if m:
            val = float(m.group(1).replace(",", "."))
            return (val, u)
    return (None, unit)

def upsert_catalog(conn, provider: str, code: str, canonical: str,
                   brand: str | None, category: str | None,
                   size_value, size_unit):
    sql = text("""
    INSERT INTO product_catalog
      (provider, product_code, canonical_name, brand, category, size_value, size_unit, updated_at)
    VALUES (:p, :c, :n, :b, :g, :sv, :su, now())
    ON CONFLICT (provider, product_code) DO UPDATE SET
      canonical_name = EXCLUDED.canonical_name,
      brand          = COALESCE(EXCLUDED.brand, product_catalog.brand),
      category       = COALESCE(EXCLUDED.category, product_catalog.category),
      size_value     = COALESCE(EXCLUDED.size_value, product_catalog.size_value),
      size_unit      = COALESCE(EXCLUDED.size_unit, product_catalog.size_unit),
      updated_at     = now()
    """)
    conn.execute(sql, {
        "p": provider, "c": code, "n": canonical,
        "b": brand, "g": category, "sv": size_value, "su": size_unit
    })

def enrich_price_doc(doc: dict) -> dict:
    provider = doc.get("provider")
    items = []
    with engine.begin() as conn:
        for it in doc.get("items", []):
            name = (it.get("product") or "").strip()
            unit = it.get("unit")
            brand = pick_brand(name)
            cat   = pick_category(name)
            sv, su = parse_size(name, unit)
            upsert_catalog(conn, provider, it["code"], name, brand, cat, sv, su)
            enriched = dict(it)
            enriched.update({
                "canonical_name": name,
                "brand": brand,
                "category": cat,
                "size_value": sv,
                "size_unit": su,
            })
            items.append(enriched)
    out = dict(doc)
    out["items"] = items
    return out

def process_message(body: str) -> None:
    d = json.loads(body)
    typ = d.get("type")
    if typ == "PriceFull":
        enriched = enrich_price_doc(d)
        return forward(enriched)
    elif typ == "PromoFull":
        return forward(d)
    else:
        raise ValueError(f"unknown type: {typ}")

def forward(doc: dict) -> None:
    sqs.send_message(QueueUrl=Q_OUT, MessageBody=json.dumps(doc, ensure_ascii=False))

if __name__ == "__main__":
    start_http_server(METRICS_PORT)
    Q_IN  = get_queue_url(IN_QUEUE)
    Q_OUT = get_queue_url(OUT_QUEUE)
    Q_DLQ = get_queue_url(DLQ_QUEUE)
    print(json.dumps({"level":"INFO","msg":"enricher started","in":IN_QUEUE,"out":OUT_QUEUE,"dlq":DLQ_QUEUE,"db":DATABASE_URL}))
    while True:
        resp = sqs.receive_message(
            QueueUrl=Q_IN, MaxNumberOfMessages=BATCH_SIZE,
            WaitTimeSeconds=WAIT_SECONDS, VisibilityTimeout=60
        )
        for m in resp.get("Messages", []):
            rh = m["ReceiptHandle"]
            body = m["Body"]
            try:
                process_message(body)
                sqs.delete_message(QueueUrl=Q_IN, ReceiptHandle=rh)
                m_ok.inc()
            except Exception as e:
                m_err.inc()
                sqs.send_message(
                    QueueUrl=Q_DLQ,
                    MessageBody=json.dumps({"error":str(e), "body": body}, ensure_ascii=False)
                )
        time.sleep(0.05)