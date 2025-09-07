import psycopg2
import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles database operations for the enricher"""
    
    def __init__(self, database_url: str):
        self.primary_database_url = database_url
        self.fallback_database_url = self._get_fallback_database_url()
        self.database_url = None
        self.connection = None
        self.connect()
    
    def _get_fallback_database_url(self) -> str:
        """Get fallback database URL for local PostgreSQL"""
        return "postgresql://postgres:8HeXmxYnvy5xu@postgres:5432/postgres"
    
    def connect(self):
        """Connect to the database with fallback"""
        # Try primary database first
        try:
            self.connection = psycopg2.connect(self.primary_database_url)
            self.database_url = self.primary_database_url
            logger.info("Connected to primary database")
            return
        except Exception as e:
            logger.warning(f"Failed to connect to primary database: {e}")
        
        # Try fallback database
        try:
            logger.info("Attempting to connect to fallback database...")
            self.connection = psycopg2.connect(self.fallback_database_url)
            self.database_url = self.fallback_database_url
            logger.info("Connected to fallback database (local PostgreSQL)")
            return
        except Exception as e:
            logger.error(f"Failed to connect to both primary and fallback databases: {e}")
            raise
    
    def _get_connection(self):
        """Get a database connection with fallback"""
        # Try primary database first
        try:
            return psycopg2.connect(self.primary_database_url)
        except Exception as e:
            logger.warning(f"Primary database connection failed: {e}")
        
        # Try fallback database
        try:
            logger.info("Using fallback database connection")
            return psycopg2.connect(self.fallback_database_url)
        except Exception as e:
            logger.error(f"Fallback database connection failed: {e}")
            raise
    
    def store_exists(self, cursor, chain_id: str, store_id: str) -> bool:
        """Check if store exists in stores table"""
        try:
            cursor.execute(
                "SELECT 1 FROM stores WHERE chain_id = %s AND store_id = %s",
                (chain_id, store_id)
            )
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Failed to check if store exists: {e}")
            return False

    def save_to_database(self, message: Dict[str, Any]) -> bool:
        import psycopg2
        conn = None
        cursor = None
        try:
            # Try to get a fresh connection
            conn = self._get_connection()
            cursor = conn.cursor()
            
            message_type = message.get('type')
            result = False
            
            if message_type in ['price_data', 'promo_data']:
                chain_id = message.get('chain_id')
                store_id = message.get('store_id')
                if chain_id and store_id:
                    if not self.store_exists(cursor, chain_id, store_id):
                        logger.warning(f"Skipping {message_type} - store does not exist: chain_id={chain_id}, store_id={store_id}")
                        return True
            
            if message_type == 'price_data':
                result = self.save_price_data(cursor, message)
            elif message_type == 'promo_data':
                result = self.save_promo_data(cursor, message)
            elif message_type == 'store_data':
                result = self.save_store_data(cursor, message)
            else:
                logger.warning(f"Unknown message type: {message_type}")
                return False
            
            if result:
                conn.commit()
                logger.info(f"Successfully saved {message_type} to database")
                return True
            else:
                conn.rollback()
                return False
                
        except Exception as e:
            logger.error(f"Database save failed: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def save_price_data(self, cursor, message: Dict[str, Any]) -> bool:
        """Save price data to items table"""
        try:
            for item in message.get('items', []):
                price_update_date = item.get('price_update_date')
                parsed_date = None
                if price_update_date:
                    try:
                        parsed_date = datetime.strptime(price_update_date, '%Y-%m-%d %H:%M:%S')
                    except:
                        try:
                            parsed_date = datetime.strptime(price_update_date, '%Y-%m-%d')
                        except:
                            pass
                
                upsert_query = """
                INSERT INTO items (
                    chain_id, store_id, item_code, item_id, item_type, item_name,
                    manufacturer_name, manufacture_country, manufacturer_item_description,
                    unit_qty, quantity, unit_of_measure, is_weighted, qty_in_package,
                    item_price, unit_of_measure_price, allow_discount, item_status,
                    item_brand, brand_confidence, brand_extraction_method, price_update_date
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (chain_id, store_id, item_code, price_update_date)
                DO UPDATE SET
                    item_name = EXCLUDED.item_name,
                    manufacturer_name = EXCLUDED.manufacturer_name,
                    item_price = EXCLUDED.item_price,
                    item_brand = EXCLUDED.item_brand,
                    brand_confidence = EXCLUDED.brand_confidence,
                    brand_extraction_method = EXCLUDED.brand_extraction_method,
                    updated_at = CURRENT_TIMESTAMP
                """
                
                cursor.execute(upsert_query, (
                    message['chain_id'],
                    message['store_id'],
                    item['item_code'],
                    item.get('item_id'),
                    item.get('item_type'),
                    item['item_name'],
                    item.get('manufacturer_name'),
                    item.get('manufacture_country'),
                    item.get('manufacturer_item_description'),
                    item.get('unit_qty'),
                    item.get('quantity'),
                    item.get('unit_of_measure'),
                    item.get('is_weighted'),
                    item.get('qty_in_package'),
                    item.get('item_price'),
                    item.get('unit_of_measure_price'),
                    item.get('allow_discount'),
                    item.get('item_status'),
                    item.get('item_brand'),
                    item.get('brand_confidence'),
                    item.get('brand_extraction_method'),
                    parsed_date
                ))
            
            items_count = len(message.get('items', []))
            logger.info(f"Processed {items_count} items for database insertion")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save price data: {e}")
            return False
    
    def save_promo_data(self, cursor, message: Dict[str, Any]) -> bool:
        """Save promotion data to discounts table"""
        try:
            for discount in message.get('discounts', []):
                promotion_update_date = self.parse_datetime(discount.get('promotion_update_date'))
                promotion_start_date = self.parse_date(discount.get('promotion_start_date'))
                promotion_end_date = self.parse_date(discount.get('promotion_end_date'))
                promotion_start_hour = self.parse_time(discount.get('promotion_start_hour'))
                promotion_end_hour = self.parse_time(discount.get('promotion_end_hour'))
                
                upsert_query = """
                INSERT INTO discounts (
                    chain_id, store_id, promotion_id, promotion_description,
                    promotion_update_date, promotion_start_date, promotion_start_hour,
                    promotion_end_date, promotion_end_hour, reward_type,
                    allow_multiple_discounts, is_weighted_promo, min_qty,
                    discounted_price, discounted_price_per_mida, min_no_of_item_offered,
                    item_code, item_type, is_gift_item, club_id,
                    additional_is_coupon, additional_gift_count, additional_is_total,
                    additional_is_active
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (chain_id, store_id, promotion_id, item_code)
                DO UPDATE SET
                    promotion_description = EXCLUDED.promotion_description,
                    promotion_update_date = EXCLUDED.promotion_update_date,
                    promotion_start_date = EXCLUDED.promotion_start_date,
                    promotion_end_date = EXCLUDED.promotion_end_date,
                    discounted_price = EXCLUDED.discounted_price,
                    updated_at = CURRENT_TIMESTAMP
                """
                
                cursor.execute(upsert_query, (
                    message['chain_id'],
                    message['store_id'],
                    discount.get('promotion_id'),
                    discount.get('promotion_description'),
                    promotion_update_date,
                    promotion_start_date,
                    promotion_start_hour,
                    promotion_end_date,
                    promotion_end_hour,
                    discount.get('reward_type'),
                    discount.get('allow_multiple_discounts'),
                    discount.get('is_weighted_promo'),
                    discount.get('min_qty'),
                    discount.get('discounted_price'),
                    discount.get('discounted_price_per_mida'),
                    discount.get('min_no_of_item_offered'),
                    discount.get('item_code'),
                    discount.get('item_type'),
                    discount.get('is_gift_item'),
                    discount.get('club_id'),
                    discount.get('additional_is_coupon'),
                    discount.get('additional_gift_count'),
                    discount.get('additional_is_total'),
                    discount.get('additional_is_active')
                ))
            
            discounts_count = len(message.get('discounts', []))
            logger.info(f"Processed {discounts_count} discounts for database insertion")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save promo data: {e}")
            return False
    
    def save_store_data(self, cursor, message: Dict[str, Any]) -> bool:
        """Save store data to stores table"""
        try:
            for store in message.get('stores', []):
                last_update_date = self.parse_date(store.get('last_update_date'))
                last_update_time = self.parse_time(store.get('last_update_time'))
                
                upsert_query = """
                INSERT INTO stores (
                    chain_id, chain_name, last_update_date, last_update_time,
                    sub_chain_id, sub_chain_name, store_id, bikoret_no, store_type,
                    store_name, address, city, zip_code
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (chain_id, store_id)
                DO UPDATE SET
                    chain_name = EXCLUDED.chain_name,
                    last_update_date = EXCLUDED.last_update_date,
                    last_update_time = EXCLUDED.last_update_time,
                    sub_chain_id = EXCLUDED.sub_chain_id,
                    sub_chain_name = EXCLUDED.sub_chain_name,
                    bikoret_no = EXCLUDED.bikoret_no,
                    store_type = EXCLUDED.store_type,
                    store_name = EXCLUDED.store_name,
                    address = EXCLUDED.address,
                    city = EXCLUDED.city,
                    zip_code = EXCLUDED.zip_code,
                    updated_at = CURRENT_TIMESTAMP
                """
                
                cursor.execute(upsert_query, (
                    store['chain_id'],
                    store['chain_name'],
                    last_update_date,
                    last_update_time,
                    store.get('sub_chain_id'),
                    store.get('sub_chain_name'),
                    store['store_id'],
                    store.get('bikoret_no'),
                    store.get('store_type'),
                    store['store_name'],
                    store.get('address'),
                    store.get('city'),
                    store.get('zip_code')
                ))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to save store data: {e}")
            self.connection.rollback()
            return False
    
    def parse_datetime(self, date_str):
        """Parse datetime string"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d %H:%M')
        except:
            try:
                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            except:
                return None
    
    def parse_date(self, date_str):
        """Parse date string"""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            return None
    
    def parse_time(self, time_str):
        """Parse time string"""
        if not time_str:
            return None
        try:
            return datetime.strptime(time_str, '%H:%M:%S').time()
        except:
            try:
                return datetime.strptime(time_str, '%H:%M').time()
            except:
                return None
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")