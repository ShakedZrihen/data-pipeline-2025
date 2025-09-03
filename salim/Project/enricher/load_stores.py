import json
import os
import psycopg2
from pathlib import Path
from datetime import datetime

def load_stores():
    """Load stores data from JSON files into the database"""
    
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("DATABASE_URL not found in .env file")
        return False
    
    stores_dir = "Stores"
    json_files = list(Path(stores_dir).glob("*.json"))
    
    print(f"Found {len(json_files)} stores JSON files")
    
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        # Clear existing data
        print("Clearing existing stores data...")
        cursor.execute("DELETE FROM stores;")
        
        total_stores = 0
        
        for json_file in json_files:
            try:
                print(f"\nProcessing: {json_file.name}")
                
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                root = data.get('Root', {})
                
                # Extract chain-level data
                chain_id = root.get('ChainID') or root.get('ChainId')
                chain_name = root.get('ChainName')
                last_update_date = root.get('LastUpdateDate')
                last_update_time = root.get('LastUpdateTime')
                
                # Parse date and time
                parsed_date = None
                parsed_time = None
                
                if last_update_date:
                    try:
                        parsed_date = datetime.strptime(last_update_date, '%Y-%m-%d').date()
                    except:
                        pass
                
                if last_update_time:
                    try:
                        # Handle time format like "01:00:00.167"
                        time_str = last_update_time.split('.')[0]  # Remove milliseconds
                        parsed_time = datetime.strptime(time_str, '%H:%M:%S').time()
                    except:
                        pass
                
                # Extract stores data
                sub_chains = root.get('SubChains', {})
                sub_chain = sub_chains.get('SubChain', {})
                
                # Handle both single and multiple sub-chains
                if isinstance(sub_chain, list):
                    sub_chains_list = sub_chain
                else:
                    sub_chains_list = [sub_chain]
                
                file_stores_count = 0
                
                for sub_chain_data in sub_chains_list:
                    sub_chain_id = sub_chain_data.get('SubChainID')
                    sub_chain_name = sub_chain_data.get('SubChainName')

                    stores = sub_chain_data.get('Stores', {}).get('Store', [])

                    # Handle both single and multiple stores
                    if isinstance(stores, list):
                        stores_list = stores
                    else:
                        stores_list = [stores]

                    for store in stores_list:
                        store_id = store.get('StoreID') or store.get('StoreId')
                        bikoret_no = store.get('BikoretNo')
                        store_type = store.get('StoreType')
                        store_name = store.get('StoreName')
                        address = store.get('Address')
                        city = store.get('City')
                        zip_code = store.get('ZipCode')

                        # Convert store_type to integer
                        try:
                            store_type_int = int(store_type) if store_type else None
                        except:
                            store_type_int = None

                        # Insert into database
                        insert_sql = """
                        INSERT INTO stores (
                            chain_id, chain_name, last_update_date, last_update_time,
                            sub_chain_id, sub_chain_name, store_id, bikoret_no, store_type,
                            store_name, address, city, zip_code
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (chain_id, store_id) DO UPDATE SET
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
                            updated_at = CURRENT_TIMESTAMP;
                        """


                        cursor.execute(insert_sql, (
                            chain_id, chain_name, parsed_date, parsed_time,
                            sub_chain_id, sub_chain_name, store_id, bikoret_no, store_type_int,
                            store_name, address, city, zip_code
                        ))

                        total_stores += 1
                        file_stores_count += 1

                print(f"Processed {file_stores_count} stores from {json_file.name}")

            except Exception as e:
                print(f"Error processing {json_file.name}: {e}")

        conn.commit()

        # Verify data was loaded
        cursor.execute("SELECT COUNT(*) FROM stores;")
        count = cursor.fetchone()[0]

        print(f"\nSuccessfully loaded {total_stores} stores into database!")
        print(f"Total stores in database: {count}")

        # Show sample data
        cursor.execute("""
            SELECT chain_name, store_name, city, zip_code
            FROM stores
            LIMIT 5;
        """)

        sample_stores = cursor.fetchall()
        print(f"\nSample stores loaded:")
        for store in sample_stores:
            print(f"  • {store[0]} - {store[1]} ({store[2]}, {store[3]})")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Failed to load stores data: {e}")
        return False

if __name__ == "__main__":
    success = load_stores()
    if success:
        print("\nStores data loading complete!")
    else:
        print("\nStores data loading failed!")
