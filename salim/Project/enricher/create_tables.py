import os
import psycopg2
from dotenv import load_dotenv

def create_tables():
    """Create stores, items, and discounts tables with proper relationships"""
    
    load_dotenv('../.env')
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("DATABASE_URL not found in .env file")
        return False
    
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Drop existing tables in correct order (due to foreign keys)
        print("Dropping existing tables...")
        cursor.execute("DROP TABLE IF EXISTS discounts CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS items CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS stores CASCADE;")
        
        # Create stores table
        print("Creating stores table...")
        stores_sql = """
        CREATE TABLE stores (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR(50) NOT NULL,
            chain_name VARCHAR(255) NOT NULL,
            last_update_date DATE,
            last_update_time TIME,
            sub_chain_id VARCHAR(50),
            sub_chain_name VARCHAR(255),
            store_id VARCHAR(50) NOT NULL,
            bikoret_no VARCHAR(10),
            store_type INTEGER,
            store_name VARCHAR(255) NOT NULL,
            address TEXT,
            city VARCHAR(100),
            zip_code VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, store_id)
        );
        """
        cursor.execute(stores_sql)
        
        # Create stores indexes
        stores_indexes = """
        CREATE INDEX idx_stores_chain_id ON stores(chain_id);
        CREATE INDEX idx_stores_store_id ON stores(store_id);
        CREATE INDEX idx_stores_chain_store ON stores(chain_id, store_id);
        CREATE INDEX idx_stores_sub_chain ON stores(sub_chain_id);
        CREATE INDEX idx_stores_city ON stores(city);
        """
        cursor.execute(stores_indexes)
        
        # Create items table (based on PriceFull files)
        print("Creating items table...")
        items_sql = """
        CREATE TABLE items (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR(50) NOT NULL,
            store_id VARCHAR(50) NOT NULL,
            item_code VARCHAR(50) NOT NULL,
            item_id VARCHAR(50),
            item_type VARCHAR(10),
            item_name VARCHAR(500) NOT NULL,
            manufacturer_name VARCHAR(255),
            manufacture_country VARCHAR(100),
            manufacturer_item_description TEXT,
            unit_qty VARCHAR(50),
            quantity DECIMAL(10,3),
            unit_of_measure VARCHAR(50),
            is_weighted BOOLEAN DEFAULT FALSE,
            qty_in_package VARCHAR(100),
            item_price DECIMAL(10,2),
            unit_of_measure_price DECIMAL(12,4),
            allow_discount BOOLEAN DEFAULT TRUE,
            item_status INTEGER,
            item_brand VARCHAR(255),
            brand_confidence DECIMAL(3,2),
            brand_extraction_method VARCHAR(50),
            price_update_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, store_id, item_code, price_update_date),
            FOREIGN KEY (chain_id, store_id) REFERENCES stores(chain_id, store_id)
        );
        """
        cursor.execute(items_sql)
        
        # Create items indexes
        items_indexes = """
        CREATE INDEX idx_items_chain_id ON items(chain_id);
        CREATE INDEX idx_items_store_id ON items(store_id);
        CREATE INDEX idx_items_item_code ON items(item_code);
        CREATE INDEX idx_items_chain_store ON items(chain_id, store_id);
        CREATE INDEX idx_items_price ON items(item_price);
        CREATE INDEX idx_items_brand ON items(item_brand);
        CREATE INDEX idx_items_manufacturer ON items(manufacturer_name);
        CREATE INDEX idx_items_price_update ON items(price_update_date);
        """
        cursor.execute(items_indexes)
        
        # Create discounts table (based on PromoFull files)
        print("Creating discounts table...")
        discounts_sql = """
        CREATE TABLE discounts (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR(50) NOT NULL,
            store_id VARCHAR(50) NOT NULL,
            promotion_id VARCHAR(50) NOT NULL,
            promotion_description TEXT,
            promotion_update_date TIMESTAMP,
            promotion_start_date DATE,
            promotion_start_hour TIME,
            promotion_end_date DATE,
            promotion_end_hour TIME,
            reward_type INTEGER,
            allow_multiple_discounts BOOLEAN DEFAULT FALSE,
            is_weighted_promo BOOLEAN DEFAULT FALSE,
            min_qty DECIMAL(10,3),
            discounted_price DECIMAL(10,2),
            discounted_price_per_mida DECIMAL(10,2),
            min_no_of_item_offered INTEGER,
            item_code VARCHAR(50),
            item_type VARCHAR(10),
            is_gift_item BOOLEAN DEFAULT FALSE,
            club_id VARCHAR(50),
            additional_is_coupon BOOLEAN DEFAULT FALSE,
            additional_gift_count INTEGER DEFAULT 0,
            additional_is_total BOOLEAN DEFAULT FALSE,
            additional_is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, store_id, promotion_id, item_code),
            FOREIGN KEY (chain_id, store_id) REFERENCES stores(chain_id, store_id)
        );
        """
        cursor.execute(discounts_sql)
        
        # Create discounts indexes
        discounts_indexes = """
        CREATE INDEX idx_discounts_chain_id ON discounts(chain_id);
        CREATE INDEX idx_discounts_store_id ON discounts(store_id);
        CREATE INDEX idx_discounts_promotion_id ON discounts(promotion_id);
        CREATE INDEX idx_discounts_item_code ON discounts(item_code);
        CREATE INDEX idx_discounts_chain_store ON discounts(chain_id, store_id);
        CREATE INDEX idx_discounts_dates ON discounts(promotion_start_date, promotion_end_date);
        CREATE INDEX idx_discounts_price ON discounts(discounted_price);
        """
        cursor.execute(discounts_indexes)
        
        # Create processed_files table for Last Run Save functionality
        print("Creating processed_files table...")
        processed_files_sql = """
        CREATE TABLE processed_files (
            id SERIAL PRIMARY KEY,
            s3_key VARCHAR(500) NOT NULL UNIQUE,
            file_type VARCHAR(50) NOT NULL,
            supermarket VARCHAR(50) NOT NULL,
            file_size BIGINT,
            s3_last_modified TIMESTAMP,
            processing_status VARCHAR(20) DEFAULT 'pending',
            processing_started_at TIMESTAMP,
            processing_completed_at TIMESTAMP,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(processed_files_sql)
        
        # Create processed_files indexes
        processed_files_indexes = """
        CREATE INDEX idx_processed_files_s3_key ON processed_files(s3_key);
        CREATE INDEX idx_processed_files_status ON processed_files(processing_status);
        CREATE INDEX idx_processed_files_supermarket ON processed_files(supermarket);
        CREATE INDEX idx_processed_files_type ON processed_files(file_type);
        CREATE INDEX idx_processed_files_modified ON processed_files(s3_last_modified);
        """
        cursor.execute(processed_files_indexes)
        
        conn.commit()
        print("All tables created successfully!")
        
        # Verify tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name IN ('stores', 'items', 'discounts', 'processed_files')
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Created tables: {', '.join(tables)}")
        
        # Show table relationships
        cursor.execute("""
            SELECT 
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_schema = 'public'
            ORDER BY tc.table_name;
        """)
        
        relationships = cursor.fetchall()
        if relationships:
            print("\nForeign key relationships:")
            for rel in relationships:
                print(f"  {rel[0]}.{rel[1]} -> {rel[2]}.{rel[3]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Failed to create tables: {e}")
        return False

if __name__ == "__main__":
    success = create_tables()
    if success:
        print("\nDatabase setup complete.")
    else:
        print("\nDatabase setup failed.")