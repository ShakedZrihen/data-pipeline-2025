import psycopg2
import os

def check_supabase():
    try:
        # Your Supabase connection
        conn = psycopg2.connect(
            "postgresql://postgres:8HeXmxYnvy5xu@db.sifzchhpypeprqfirrdb.supabase.co:5432/postgres"
        )
        cursor = conn.cursor()
        
        print("✅ Connected to Supabase!")
        
        # Check table counts
        tables = ['stores', 'items', 'discounts', 'processed_files']
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table}: {count} rows")
            except:
                print(f"{table}: Table doesn't exist")
        
        # Show sample data
        print("\n--- Sample Data ---")
        for table in tables:
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT 2")
                rows = cursor.fetchall()
                if rows:
                    print(f"\n{table} (first 2 rows):")
                    for row in rows:
                        print(f"  {row}")
            except:
                pass
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    check_supabase()
