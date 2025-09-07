import os, json, time, hashlib, traceback
from decimal import Decimal
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from pydantic import BaseModel, Field, ValidationError, field_validator
from prometheus_client import Counter, Gauge, start_http_server
from sqlalchemy import create_engine, text

OUT_QUEUE = os.getenv("OUT_QUEUE", "price-events")
DLQ_QUEUE = os.getenv("DLQ_QUEUE", "price-events-dlq")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/salim_db")
DB_URL         = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/salim_db")
SQS_ENDPOINT   = os.getenv("SQS_ENDPOINT", "http://localstack:4566")
REGION         = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
IN_QUEUE       = os.getenv("IN_QUEUE", "price-events")
DLQ_QUEUE      = os.getenv("DLQ_QUEUE", f"{IN_QUEUE}-dlq")
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "10"))
WAIT_SECONDS   = int(os.getenv("WAIT_SECONDS", "10"))
METRICS_PORT   = int(os.getenv("METRICS_PORT", "9108"))

sqs = boto3.client("sqs",
    endpoint_url=SQS_ENDPOINT,
    region_name=REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID","test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY","test"),
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

m_msgs_ok   = Counter("consumer_messages_ok_total", "Messages successfully processed")
m_msgs_err  = Counter("consumer_messages_error_total", "Messages failed")
m_rows_ins  = Counter("consumer_rows_inserted_total", "Rows inserted to DB")
m_lag_sec   = Gauge("consumer_doc_lag_seconds", "Doc lag (now - doc_ts) seconds")


class PriceItem(BaseModel):
    code: str
    product: str
    price: Decimal
    unit: str | None = None

    @field_validator("price", mode="before")
    def _price_to_decimal(cls, v):
        return Decimal(str(v))

class PromoItem(BaseModel):
    description: str | None = None
    start: str | None = None
    end:   str | None = None
    price: Decimal | None = None
    rate:  Decimal | None = None
    quantity: int | None = None
    items: list[str] = Field(default_factory=list)

    @field_validator("price","rate", mode="before")
    def _decimals(cls, v):
        return None if v in (None, "") else Decimal(str(v))

class BaseDoc(BaseModel):
    provider: str
    branch: str
    type: str
    timestamp: str
    part: int | None = None
    parts: int | None = None

    def parsed_ts(self) -> datetime:
        return datetime.strptime(self.timestamp, "%Y-%m-%d_%H:%M").replace(tzinfo=timezone.utc)

class PriceDoc(BaseDoc):
    items: list[PriceItem]

class PromoDoc(BaseDoc):
    promotions: list[PromoItem]


def ensure_supermarket(conn, provider: str, branch_code: str, branch_name: str|None = None):
    sql = text("""
        INSERT INTO supermarket (provider, branch_code, name, branch_name, website)
        VALUES (:provider, :branch_code, :name, :branch_name, :website)
        ON CONFLICT (provider, branch_code) DO NOTHING
    """)

    PROVIDER_META = {
        "Yohananof": {"name": "Yohananof", "website": "https://www.yochananof.co.il/"},
        "RamiLevy":  {"name": "Rami Levi", "website": "https://www.rami-levy.co.il/"},
        "OsherAd":   {"name": "Osher Ad", "website": "https://www.osherad.co.il/"},
    }
    meta = PROVIDER_META.get(provider, {"name": provider, "website": None})
    conn.execute(sql, {
        "provider": provider,
        "branch_code": branch_code,
        "name": meta["name"],
        "branch_name": branch_name,
        "website": meta["website"],
    })

def _parse_qty(v):
    if v in (None, "", "null"):
        return None
    try:
        return int(Decimal(str(v)))
    except Exception:
        try:
            return int(float(str(v).replace(",", ".")))
        except Exception:
            return None

def get_queue_url(name: str) -> str:
    try:
        return sqs.get_queue_url(QueueName=name)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        sqs.create_queue(QueueName=name)
        return sqs.get_queue_url(QueueName=name)["QueueUrl"]

def upsert_price_items(conn, doc):
    ts = doc["timestamp"].replace("_", " ") + ":00+00"
    rows = [{
        "provider": doc["provider"],
        "branch_code": doc.get("branch_code") or doc.get("branch"),
        "code": it["code"],
        "name": it["product"],
        "unit": it.get("unit"),
        "price": Decimal(str(it["price"])),
        "ts": ts,
    } for it in doc.get("items", [])]

    if not rows:
        return 0

    sql = text("""
        INSERT INTO price_item
        (provider, branch_code, product_code, product_name, unit, price, ts)
        VALUES (:provider, :branch_code, :code, :name, :unit, :price, :ts)
        ON CONFLICT (provider, branch_code, product_code, ts)
        DO UPDATE SET
          product_name = EXCLUDED.product_name,
          unit         = EXCLUDED.unit,
          price        = EXCLUDED.price
    """)
    conn.execute(sql, rows)
    return len(rows)

def upsert_promo_items(conn, doc):
    rows = []
    for p in doc.get("promotions", []):
        rows += [{
            "provider": doc["provider"],
            "branch_code": doc.get("branch_code") or doc.get("branch"),
            "code": code,
            "description": p.get("description"),
            "start_ts": (p.get("start") or "").replace("_", " ") + ":00+00" if p.get("start") else None,
            "end_ts":   (p.get("end") or "").replace("_", " ") + ":00+00" if p.get("end") else None,
            "price": Decimal(str(p["price"])) if p.get("price") is not None else None,
            "rate":  Decimal(str(p["rate"]))  if p.get("rate")  is not None else None,
            "quantity": _parse_qty(p.get("quantity")),
        } for code in (p.get("items") or [])]

    if not rows:
        return 0

    sql = text("""
        INSERT INTO promo_item
        (provider, branch_code, product_code, description, start_ts, end_ts, price, rate, quantity)
        VALUES (:provider, :branch_code, :code, :description, :start_ts, :end_ts, :price, :rate, :quantity)
        ON CONFLICT (provider, branch_code, product_code, description, start_ts, end_ts)
        DO UPDATE SET
          price    = EXCLUDED.price,
          rate     = EXCLUDED.rate,
          quantity = EXCLUDED.quantity
    """)
    conn.execute(sql, rows)
    return len(rows)

def upsert_ingested(conn, msg_id, doc):
    sql = text("""
        INSERT INTO ingested_message
        (message_id, provider, branch_code, type, ts_doc, body)
        VALUES (:mid, :provider, :branch_code, :type, :ts_doc, CAST(:body AS jsonb))
        ON CONFLICT (message_id) DO NOTHING
    """)
    conn.execute(sql, {
        "mid": msg_id,
        "provider": doc.get("provider"),
        "branch_code": doc.get("branch_code") or doc.get("branch"),
        "type": doc.get("type"),
        "ts_doc": doc.get("timestamp").replace("_", " ") + ":00+00" if doc.get("timestamp") else None,
        "body": json.dumps(doc, ensure_ascii=False),
    })

def process_message(body: str, conn) -> int:
    doc = json.loads(body)
    provider = doc.get("provider")
    branch_code = doc.get("branch_code") or doc.get("branch")
    branch_name = doc.get("branch")

    ensure_supermarket(conn, provider, branch_code, branch_name)
    upsert_ingested(conn, doc.get("message_id", str(time.time())), doc)
    if doc.get("type") == "PriceFull":
        return upsert_price_items(conn, doc)
    elif doc.get("type") == "PromoFull":
        return upsert_promo_items(conn, doc)
    else:
        raise ValueError(f"unknown type: {doc.get('type')}")

def main():
    start_http_server(METRICS_PORT)

    in_q = get_queue_url(IN_QUEUE)
    dlq = get_queue_url(DLQ_QUEUE)
    print(json.dumps({"level":"INFO","msg":"consumer started","queue":IN_QUEUE,"dlq":DLQ_QUEUE,"db":DATABASE_URL}))

    while True:
        resp = sqs.receive_message(
            QueueUrl=in_q,
            MaxNumberOfMessages=BATCH_SIZE,
            WaitTimeSeconds=WAIT_SECONDS,
            VisibilityTimeout=60
        )
        for m in resp.get("Messages", []):
            receipt = m["ReceiptHandle"]
            body = m["Body"]
            try:
                with engine.begin() as conn:
                    inserted = process_message(body, conn)

                sqs.delete_message(QueueUrl=in_q, ReceiptHandle=receipt)
                m_msgs_ok.inc()
                m_rows_ins.inc(inserted)

                try:
                    d = json.loads(body)
                    ts = datetime.strptime(d["timestamp"],
                                           "%Y-%m-%d_%H:%M").replace(
                        tzinfo=timezone.utc)
                    m_lag_sec.set(
                        (datetime.now(timezone.utc) - ts).total_seconds())
                except Exception:
                    pass


            except Exception as e:

                m_msgs_err.inc()
                print(json.dumps({"level": "ERROR", "msg": "processing_error",
                                  "details": str(e)}))
                sqs.send_message(
                    QueueUrl=dlq,
                    MessageBody=json.dumps(
                        {"error": str(e), "trace": traceback.format_exc(),
                         "body": body},
                        ensure_ascii=False
                    )
                )
                sqs.delete_message(QueueUrl=in_q, ReceiptHandle=receipt)
        time.sleep(0.1)

if __name__ == "__main__":
    main()