import json, datetime as dt
import pika
from sqlalchemy.orm import Session
from shared.config import settings
from shared.mq import channel as mq_channel
from db import Base, engine, SessionLocal
from models import Supermarket, Product
from openai_enricher import enrich_with_openai

def ensure_schema():
    Base.metadata.create_all(bind=engine)

def upsert_supermarket(db: Session, supermarket_name: str):
    # First try to find by name
    sm = db.query(Supermarket).filter(Supermarket.name==supermarket_name).first()
    if not sm:
        # Create new supermarket with auto-increment ID
        sm = Supermarket(name=supermarket_name)
        db.add(sm)
        db.commit()
        db.refresh(sm)
    return sm

def save_product(db: Session, data: dict):
    # Convert collected_at
    col = data.get("collected_at")
    if isinstance(col, str):
        try:
            data["collected_at"] = dt.datetime.fromisoformat(col.replace("Z","+00:00"))
        except Exception:
            data["collected_at"] = None
    p = Product(**{
        "supermarket_id": data["supermarket_id"],
        "barcode": data.get("barcode"),
        "canonical_name": data.get("canonical_name") or data.get("name") or "Unknown",
        "brand": data.get("brand"),
        "category": data.get("category"),
        "size_value": data.get("size_value"),
        "size_unit": data.get("size_unit"),
        "price": data.get("price", 0.0),
        "currency": data.get("currency","ILS"),
        "promo_price": data.get("promo_price"),
        "promo_text": data.get("promo_text"),
        "in_stock": bool(data.get("in_stock", True)),
        "collected_at": data.get("collected_at")
    })
    db.add(p)
    db.commit()

def callback(ch, method, properties, body):
    try:
        data = json.loads(body.decode("utf-8"))
        data = enrich_with_openai(data)
        with SessionLocal() as db:
            supermarket_name = data.get("supermarket_name", "Unknown")
            supermarket = upsert_supermarket(db, supermarket_name)
            data["supermarket_id"] = supermarket.supermarket_id
            save_product(db, data)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Failed to process message:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

if __name__ == "__main__":
    ensure_schema()
    conn, ch = mq_channel()
    ch.basic_qos(prefetch_count=10)
    ch.basic_consume(queue=settings.rabbitmq_queue, on_message_callback=callback)
    print("Enricher listening...")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        conn.close()
