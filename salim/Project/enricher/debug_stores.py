import os
import psycopg2
from dotenv import load_dotenv

load_dotenv('../.env')
database_url = os.getenv('DATABASE_URL')

if not database_url:
    print("DATABASE_URL not found")
    exit(1)

try:
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor()
    
    # Check total stores
    cursor.execute("SELECT COUNT(*) FROM stores")
    total = cursor.fetchone()[0]
    print(f"Total stores in database: {total}")
    
    # Check specific chain
    cursor.execute("SELECT COUNT(*) FROM stores WHERE chain_id = '7290103152017'")
    chain_count = cursor.fetchone()[0]
    print(f"Stores with chain_id 7290103152017: {chain_count}")
    
    # Check if it exists with different format
    cursor.execute("SELECT DISTINCT chain_id FROM stores WHERE chain_id LIKE '%7290103152017%'")
    chains = cursor.fetchall()
    print(f"Chain IDs containing 7290103152017: {chains}")
    
    # Check store 032 in any chain
    cursor.execute("SELECT chain_id, store_id, store_name FROM stores WHERE store_id IN ('032', '32') LIMIT 5")
    stores = cursor.fetchall()
    print(f"Stores with ID 032 or 32: {stores}")
    
    # Show some sample stores
    cursor.execute("SELECT chain_id, store_id, store_name FROM stores LIMIT 5")
    samples = cursor.fetchall()
    print(f"Sample stores: {samples}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
