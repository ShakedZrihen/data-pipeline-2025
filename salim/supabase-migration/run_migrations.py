#!/usr/bin/env python3
"""
Database Migration Runner and Validator
Runs Supabase PostgreSQL migrations and validates the schema
"""

import os
import sys
import psycopg2
from psycopg2 import sql
import logging
from typing import List, Dict, Any
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MigrationRunner:
    """Handles database migrations and validation"""
    
    def __init__(self, connection_string: str = None):
        """
        Initialize migration runner
        
        Args:
            connection_string: PostgreSQL connection string
                              If None, will try to build from environment variables
        """
        self.connection_string = connection_string or self._build_connection_string()
        self.migration_dir = Path(__file__).parent
        
    def _build_connection_string(self) -> str:
        """Build connection string from environment variables"""
        # Try Supabase environment variables first
        supabase_host = os.getenv('SUPABASE_DB_HOST')
        supabase_password = os.getenv('SUPABASE_DB_PASSWORD')
        supabase_port = os.getenv('SUPABASE_DB_PORT', '5432')
        
        if supabase_host and supabase_password:
            return f"postgresql://postgres:{supabase_password}@{supabase_host}:{supabase_port}/postgres"
        
        # Fallback to standard PostgreSQL variables
        host = os.getenv('PGHOST', 'localhost')
        port = os.getenv('PGPORT', '5432')
        database = os.getenv('PGDATABASE', 'postgres')
        user = os.getenv('PGUSER', 'postgres')
        password = os.getenv('PGPASSWORD', '')
        
        if not password:
            raise ValueError("Database password not found. Set SUPABASE_DB_PASSWORD or PGPASSWORD")
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    def get_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(self.connection_string)
            conn.autocommit = True
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def run_migration_file(self, filepath: Path) -> bool:
        """Run a single migration file"""
        try:
            logger.info(f"Running migration: {filepath.name}")
            
            with open(filepath, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql_content)
            
            logger.info(f"Successfully applied: {filepath.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply {filepath.name}: {e}")
            return False
    
    def run_all_migrations(self) -> bool:
        """Run all migration files in order"""
        migration_files = [
            "001_create_supermarket_data_table.sql",
            "002_create_constraints.sql", 
            "003_create_indexes.sql"
        ]
        
        logger.info("Starting database migrations...")
        
        for filename in migration_files:
            filepath = self.migration_dir / filename
            if not filepath.exists():
                logger.error(f"Migration file not found: {filename}")
                return False
            
            if not self.run_migration_file(filepath):
                return False
        
        logger.info("All migrations completed successfully!")
        return True
    
    def validate_schema(self) -> bool:
        """Validate that the schema was created correctly"""
        logger.info("Validating database schema...")
        
        validations = [
            self._validate_table_exists,
            self._validate_columns,
            self._validate_constraints,
            self._validate_indexes
        ]
        
        for validation in validations:
            if not validation():
                return False
        
        logger.info("Schema validation passed!")
        return True
    
    def _validate_table_exists(self) -> bool:
        """Check if supermarket_data table exists"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = 'supermarket_data'
                        );
                    """)
                    exists = cur.fetchone()[0]
                    
                    if exists:
                        logger.info("Table 'supermarket_data' exists")
                        return True
                    else:
                        logger.error("Table 'supermarket_data' not found")
                        return False
                        
        except Exception as e:
            logger.error(f" Error checking table existence: {e}")
            return False
    
    def _validate_columns(self) -> bool:
        """Validate all expected columns exist with correct types"""
        expected_columns = {
            'id': 'bigint',
            'provider': 'character varying',
            'branch': 'character varying',
            'file_type': 'character varying',
            'file_timestamp': 'timestamp with time zone',
            'product_name': 'character varying',
            'product_code': 'character varying',
            'manufacturer': 'character varying',
            'price': 'numeric',
            'unit': 'character varying',
            'unit_price': 'numeric',
            'min_quantity': 'numeric',
            'price_per_kg': 'numeric',
            'price_per_liter': 'numeric',
            'is_promotion': 'boolean',
            'is_kosher': 'boolean',
            'category': 'character varying',
            'message_id': 'character varying',
            'total_items_in_file': 'integer',
            'processed_at': 'timestamp with time zone',
            'created_at': 'timestamp with time zone',
            'updated_at': 'timestamp with time zone'
        }
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns 
                        WHERE table_name = 'supermarket_data'
                        ORDER BY ordinal_position;
                    """)
                    actual_columns = {row[0]: row[1] for row in cur.fetchall()}
                    
                    missing_columns = set(expected_columns.keys()) - set(actual_columns.keys())
                    if missing_columns:
                        logger.error(f" Missing columns: {missing_columns}")
                        return False
                    
                    type_mismatches = []
                    for col, expected_type in expected_columns.items():
                        if col in actual_columns and actual_columns[col] != expected_type:
                            type_mismatches.append(f"{col}: expected {expected_type}, got {actual_columns[col]}")
                    
                    if type_mismatches:
                        logger.error(f" Column type mismatches: {type_mismatches}")
                        return False
                    
                    logger.info(f" All {len(expected_columns)} columns validated")
                    return True
                    
        except Exception as e:
            logger.error(f" Error validating columns: {e}")
            return False
    
    def _validate_constraints(self) -> bool:
        """Validate constraints exist"""
        expected_constraints = [
            'supermarket_data_pkey',
            'supermarket_data_provider_branch_file_type_file_timestamp_p_key',
            'check_price_positive',
            'check_file_type_valid'
        ]
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT constraint_name
                        FROM information_schema.table_constraints
                        WHERE table_name = 'supermarket_data';
                    """)
                    actual_constraints = [row[0] for row in cur.fetchall()]
                    
                    missing_constraints = []
                    for constraint in expected_constraints:
                        if constraint not in actual_constraints:
                            missing_constraints.append(constraint)
                    
                    if missing_constraints:
                        logger.error(f" Missing constraints: {missing_constraints}")
                        return False
                    
                    logger.info(f" All key constraints validated")
                    return True
                    
        except Exception as e:
            logger.error(f" Error validating constraints: {e}")
            return False
    
    def _validate_indexes(self) -> bool:
        """Validate indexes exist"""
        expected_indexes = [
            'supermarket_data_pkey',
            'idx_supermarket_data_provider_branch',
            'idx_supermarket_data_file_type',
            'idx_supermarket_data_processed_at',
            'idx_supermarket_data_price'
        ]
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT indexname
                        FROM pg_indexes 
                        WHERE tablename = 'supermarket_data';
                    """)
                    actual_indexes = [row[0] for row in cur.fetchall()]
                    
                    missing_indexes = []
                    for index in expected_indexes:
                        if index not in actual_indexes:
                            missing_indexes.append(index)
                    
                    if missing_indexes:
                        logger.error(f" Missing indexes: {missing_indexes}")
                        return False
                    
                    logger.info(f" All key indexes validated")
                    return True
                    
        except Exception as e:
            logger.error(f" Error validating indexes: {e}")
            return False
    
    def test_data_insertion(self) -> bool:
        """Test inserting and querying sample data"""
        logger.info("Testing data insertion...")
        
        test_data = {
            'provider': 'test_provider',
            'branch': 'test_branch',
            'file_type': 'pricesFull',
            'file_timestamp': '2025-09-03T18:00:00Z',
            'product_name': 'test_product',
            'product_code': 'TEST123',
            'manufacturer': 'Test Manufacturer',
            'price': 9.99,
            'unit': 'unit',
            'unit_price': None,
            'min_quantity': None,
            'price_per_kg': None,
            'price_per_liter': None,
            'is_promotion': False,
            'is_kosher': None,
            'category': 'test',
            'message_id': 'test_message_123',
            'total_items_in_file': 1
        }
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Insert test data
                    insert_sql = """
                        INSERT INTO supermarket_data 
                        (provider, branch, file_type, file_timestamp, product_name, 
                         product_code, manufacturer, price, unit, is_promotion, 
                         category, message_id, total_items_in_file)
                        VALUES (%(provider)s, %(branch)s, %(file_type)s, %(file_timestamp)s, 
                               %(product_name)s, %(product_code)s, %(manufacturer)s, 
                               %(price)s, %(unit)s, %(is_promotion)s, %(category)s, 
                               %(message_id)s, %(total_items_in_file)s)
                        RETURNING id;
                    """
                    cur.execute(insert_sql, test_data)
                    test_id = cur.fetchone()[0]
                    
                    # Query test data
                    cur.execute("SELECT * FROM supermarket_data WHERE id = %s", (test_id,))
                    result = cur.fetchone()
                    
                    if result:
                        logger.info(" Data insertion and retrieval successful")
                        
                        # Clean up test data
                        cur.execute("DELETE FROM supermarket_data WHERE id = %s", (test_id,))
                        logger.info(" Test data cleaned up")
                        
                        return True
                    else:
                        logger.error(" Failed to retrieve inserted test data")
                        return False
                        
        except Exception as e:
            logger.error(f" Error testing data insertion: {e}")
            return False


def main():
    """Main function to run migrations and validation"""
    logger.info("Starting database setup and validation...")
    
    try:
        # Initialize migration runner
        runner = MigrationRunner()
        
        # Run migrations
        if not runner.run_all_migrations():
            logger.error(" Migration failed")
            sys.exit(1)
        
        # Validate schema
        if not runner.validate_schema():
            logger.error(" Schema validation failed")
            sys.exit(1)
        
        # Test data operations
        if not runner.test_data_insertion():
            logger.error(" Data insertion test failed")
            sys.exit(1)
        
        logger.info("Database is ready to go! All tests passed.")
        
    except Exception as e:
        logger.error(f" Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()