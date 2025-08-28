import os
import json
import time
import boto3
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, UniqueConstraint, DateTime, func
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from botocore.exceptions import ClientError
from pydantic import BaseModel, ValidationError, conlist
from typing import List
from datetime import datetime
from dateutil import parser as dtparser 
# --- Pydantic Models for Validation ---
class Item(BaseModel):
    product_id: str
    product: str
    price: float
    unit: str

class MessagePayload(BaseModel):
    provider: str
    branch: str
    type: str
    timestamp: str
    items: conlist(Item, min_length=1)


# --- SQLAlchemy Models ---
Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(String, index=True, nullable=False)
    product_name = Column(String, nullable=False) # שם העמודה הנכון
    provider = Column(String, nullable=False)
    branch = Column(String, nullable=False)
    prices = relationship("Price", back_populates="product")
    __table_args__ = (UniqueConstraint('product_id', 'provider', 'branch', name='_product_provider_branch_uc'),)


class Price(Base):
    __tablename__ = 'prices'
    id = Column(Integer, primary_key=True, index=True)
    product_key = Column(Integer, ForeignKey('products.id'), nullable=False, index=True)
    price = Column(Float, nullable=False)
    provider = Column(String, nullable=False, index=True)
    branch = Column(String, nullable=False, index=True)
    batch_ts = Column(DateTime(timezone=True), nullable=False, index=True)  # חדש: חותמת זמן של ההודעה
    product = relationship("Product", back_populates="prices")
    __table_args__ = (
        UniqueConstraint('product_key', 'batch_ts', name='_price_product_batch_uc'),
    )

# --- Database and SQS Setup ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@db:5432/salim_db")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
QUEUE_NAME = os.getenv("QUEUE_NAME", "prices-queue")
DLQ_NAME = os.getenv("DLQ_NAME", f"{QUEUE_NAME}-dlq")

_dlq_url_cache = None
def get_dlq_url():
    global _dlq_url_cache
    if not _dlq_url_cache:
        _dlq_url_cache = sqs.get_queue_url(QueueName=DLQ_NAME)['QueueUrl']
    return _dlq_url_cache

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

sqs = boto3.client('sqs', endpoint_url=AWS_ENDPOINT_URL, region_name="us-east-1")
Base.metadata.create_all(bind=engine)

def process_messages(queue_url, messages):
    """Processes a batch of messages from the queue, now with validation."""
    print(f"Received {len(messages)} messages. Processing...")

    with SessionLocal() as session:
        valid_messages_for_deletion = []
        for msg in messages:
            try:
                payload = MessagePayload.parse_raw(msg['Body'])
                provider = payload.provider
                branch = payload.branch
                batch_ts = datetime.fromisoformat(payload.timestamp.replace('Z', '+00:00'))
                print(f"Processing data for provider '{provider}', branch '{branch}'...")

                for item in payload.items:
                    product = session.query(Product).filter_by(
                        product_id=item.product_id,
                        provider=provider,
                        branch=branch
                    ).first()

                    if not product:
                        # --- התיקון המרכזי כאן ---
                        product = Product(
                            product_id=item.product_id,
                            product_name=item.product, # השתמשנו ב-product_name
                            provider=provider,
                            branch=branch
                        )
                        session.add(product)
                        session.flush()
                        print(f"  - Creating new product with ID {item.product_id}")
                    existing = session.query(Price).filter_by(
                        product_key=product.id, batch_ts=batch_ts
                    ).first()
                    if existing:
                        print(f"  - Skipping duplicate price for {item.product_id} at {batch_ts.isoformat()}")
                    else:
                        new_price = Price(
                            product_key=product.id,
                            price=item.price,
                            provider=provider,
                            branch=branch,
                            batch_ts=batch_ts
                        )
                        session.add(new_price)
                        print(f"  - Adding new price {item.price} for product ID {item.product_id}")
                valid_messages_for_deletion.append(msg)

            except ValidationError as e:
                print(f"Validation Error for message ID {msg['MessageId']}: {e}. Message will be sent to DLQ after retries.")
                continue
            except Exception as e:
                print(f"An unexpected error occurred processing message ID {msg['MessageId']}: {e}")
                session.rollback()
                continue
        
        session.commit()
        return valid_messages_for_deletion


def main():
    """Main polling loop."""
    queue_url = None
    while not queue_url:
        try:
            queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)['QueueUrl']
            print(f"Successfully connected to queue: {queue_url}")
        except ClientError as e:
            print(f"Queue '{QUEUE_NAME}' not found yet. Retrying in 5 seconds...")
            time.sleep(5)

    print("Consumer started. Polling for messages...")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )
            
            messages = response.get('Messages', [])
            if messages:
                processed_successfully = process_messages(queue_url, messages)

                if processed_successfully:
                    entries = [{'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']} for msg in processed_successfully]
                    sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
            else:
                print("No messages received, polling again...")

        except ClientError as e:
            print(f"Error polling SQS: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()