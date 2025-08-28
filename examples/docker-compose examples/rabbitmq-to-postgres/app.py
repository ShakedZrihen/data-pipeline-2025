import pika
import psycopg2
import json
import time
import os
from datetime import datetime

def create_postgres_connection():
    """Create PostgreSQL connection"""
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            connection = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'postgres'),
                database=os.getenv('POSTGRES_DB', 'pricedb'),
                user=os.getenv('POSTGRES_USER', 'postgres'),
                password=os.getenv('POSTGRES_PASSWORD', 'postgres'),
                port=5432
            )
            return connection
        except Exception as e:
            retry_count += 1
            print(f"Failed to connect to PostgreSQL (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(2)
    
    raise Exception("Failed to connect to PostgreSQL after maximum retries")

def create_rabbitmq_connection():
    """Create RabbitMQ connection"""
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host='rabbitmq',
                    port=5672,
                    virtual_host='/',
                    credentials=pika.PlainCredentials('guest', 'guest')
                )
            )
            return connection
        except Exception as e:
            retry_count += 1
            print(f"Failed to connect to RabbitMQ (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(2)
    
    raise Exception("Failed to connect to RabbitMQ after maximum retries")

def setup_database_table(pg_conn):
    """Create the price_items, stores, and product_store_availability tables if they don't exist"""
    cursor = pg_conn.cursor()
    
    # Create stores table
    create_stores_table = """
    CREATE TABLE IF NOT EXISTS stores (
        id SERIAL PRIMARY KEY,
        store_id VARCHAR(50) UNIQUE NOT NULL,
        chain_id VARCHAR(50),
        store_name VARCHAR(255),
        store_type VARCHAR(100),
        city VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create price_items table
    create_price_items_table = """
    CREATE TABLE IF NOT EXISTS price_items (
        id SERIAL PRIMARY KEY,
        source_file VARCHAR(255),
        processed_at TIMESTAMP,
        item_code VARCHAR(50),
        item_name TEXT,
        manufacturer_name VARCHAR(255),
        item_price DECIMAL(10,2),
        unit_of_measure_price DECIMAL(10,4),
        quantity DECIMAL(10,2),
        unit_qty VARCHAR(50),
        unit_of_measure VARCHAR(50),
        price_update_date TIMESTAMP,
        item_status INTEGER,
        allow_discount INTEGER,
        is_weighted INTEGER,
        item_id VARCHAR(50),
        raw_data JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create product_store_availability table
    create_availability_table = """
    CREATE TABLE IF NOT EXISTS product_store_availability (
        id SERIAL PRIMARY KEY,
        price_item_id INTEGER REFERENCES price_items(id) ON DELETE CASCADE,
        store_id INTEGER REFERENCES stores(id) ON DELETE CASCADE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(price_item_id, store_id)
    );
    """
    
    cursor.execute(create_stores_table)
    cursor.execute(create_price_items_table)
    cursor.execute(create_availability_table)
    pg_conn.commit()
    cursor.close()
    print("Database tables 'price_items', 'stores', and 'product_store_availability' ready")

def get_or_create_store(pg_conn, store_id, chain_id):
    """Get existing store or create a new one, returns store database ID"""
    cursor = pg_conn.cursor()
    
    try:
        # Check if store exists
        cursor.execute("SELECT id FROM stores WHERE store_id = %s", (store_id,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # Basic chain mapping (can be expanded)
        chain_names = {
            "7290055700007": "Super Pharm",
            "7290058140886": "Rami Levy",
            "7290103152017": "Yochananof",
            "7290873255550": "Mega",
            "7290058108879": "Osher Ad"
        }
        
        chain_name = chain_names.get(chain_id, f"Chain {chain_id}")
        store_name = f"{chain_name} Store {store_id}"
        store_type = "supermarket"
        city = "Unknown"  # Could be enhanced with store location data
        
        insert_query = """
        INSERT INTO stores (store_id, chain_id, store_name, store_type, city)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """
        
        cursor.execute(insert_query, (store_id, chain_id, store_name, store_type, city))
        store_db_id = cursor.fetchone()[0]
        pg_conn.commit()
        
        print(f"Created new store: {store_name} ({store_id})")
        return store_db_id
        
    except Exception as e:
        print(f"Error creating store: {e}")
        pg_conn.rollback()
        return None
    finally:
        cursor.close()

def create_product_store_availability(pg_conn, price_item_id, store_db_id):
    """Create product-store availability relationship"""
    cursor = pg_conn.cursor()
    
    try:
        insert_query = """
        INSERT INTO product_store_availability (price_item_id, store_id)
        VALUES (%s, %s)
        ON CONFLICT (price_item_id, store_id) DO NOTHING
        """
        
        cursor.execute(insert_query, (price_item_id, store_db_id))
        pg_conn.commit()
        
    except Exception as e:
        print(f"Error creating product-store availability: {e}")
        pg_conn.rollback()
    finally:
        cursor.close()

def insert_price_item(pg_conn, message_data):
    """Insert a price item into PostgreSQL and create store relationship"""
    cursor = pg_conn.cursor()
    
    try:
        item_data = message_data['item_data']
        
        # Parse price update date
        price_update_date = None
        if 'PriceUpdateDate' in item_data:
            try:
                price_update_date = datetime.strptime(item_data['PriceUpdateDate'], '%Y-%m-%d %H:%M:%S')
            except:
                pass
        
        # Parse processed timestamp
        processed_at = None
        if 'timestamp' in message_data:
            try:
                processed_at = datetime.fromisoformat(message_data['timestamp'].replace('Z', '+00:00'))
            except:
                pass
        
        insert_query = """
        INSERT INTO price_items (
            source_file, processed_at, item_code, item_name, manufacturer_name,
            item_price, unit_of_measure_price, quantity, unit_qty, unit_of_measure,
            price_update_date, item_status, allow_discount, is_weighted, item_id, raw_data
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        
        values = (
            message_data.get('source_file'),
            processed_at,
            item_data.get('ItemCode'),
            item_data.get('ItemName'),
            item_data.get('ManufacturerName'),
            float(item_data.get('ItemPrice', 0)) if item_data.get('ItemPrice') else None,
            float(item_data.get('UnitOfMeasurePrice', 0)) if item_data.get('UnitOfMeasurePrice') else None,
            float(item_data.get('Quantity', 0)) if item_data.get('Quantity') else None,
            item_data.get('UnitQty'),
            item_data.get('UnitOfMeasure'),
            price_update_date,
            int(item_data.get('ItemStatus', 0)) if item_data.get('ItemStatus') else None,
            int(item_data.get('AllowDiscount', 0)) if item_data.get('AllowDiscount') else None,
            int(item_data.get('bIsWeighted', 0)) if item_data.get('bIsWeighted') else None,
            item_data.get('ItemId'),
            json.dumps(item_data)
        )
        
        cursor.execute(insert_query, values)
        price_item_id = cursor.fetchone()[0]
        pg_conn.commit()
        
        # Extract store information from message data
        store_id = message_data.get('store_id')
        chain_id = message_data.get('chain_id')
        
        if store_id and chain_id:
            # Get or create store
            store_db_id = get_or_create_store(pg_conn, store_id, chain_id)
            
            if store_db_id:
                # Create product-store availability relationship
                create_product_store_availability(pg_conn, price_item_id, store_db_id)
        
        return True
        
    except Exception as e:
        print(f"Error inserting price item: {e}")
        pg_conn.rollback()
        return False
    finally:
        cursor.close()

def process_message(ch, method, properties, body, pg_conn):
    """Process a message from RabbitMQ"""
    try:
        message_data = json.loads(body)
        
        success = insert_price_item(pg_conn, message_data)
        
        if success:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"Processed message from {message_data.get('source_file', 'unknown')}")
        else:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            print("Failed to process message, requeuing...")
            
    except Exception as e:
        print(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main():
    queue_name = os.getenv('RABBITMQ_QUEUE', 'price-items')
    
    print("Starting RabbitMQ to PostgreSQL consumer...")
    print(f"Queue: {queue_name}")
    
    # Wait for services to be ready
    time.sleep(20)
    
    pg_conn = create_postgres_connection()
    setup_database_table(pg_conn)
    
    rabbitmq_conn = create_rabbitmq_connection()
    channel = rabbitmq_conn.channel()
    
    # Declare queue to ensure it exists
    channel.queue_declare(queue=queue_name, durable=True)
    
    # Set up fair dispatch
    channel.basic_qos(prefetch_count=1)
    
    # Set up callback function
    callback = lambda ch, method, properties, body: process_message(
        ch, method, properties, body, pg_conn
    )
    
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    print("Waiting for messages. To exit press CTRL+C")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Stopping consumer...")
        channel.stop_consuming()
        rabbitmq_conn.close()
        pg_conn.close()

if __name__ == "__main__":
    main()