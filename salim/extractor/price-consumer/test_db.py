import os
import psycopg2
from dotenv import load_dotenv

# Load .env file
load_dotenv()

host = os.getenv("SUPABASE_HOST")
port = os.getenv("SUPABASE_PORT")
database = os.getenv("SUPABASE_DB")
user = os.getenv("SUPABASE_USER")
password = os.getenv("SUPABASE_PASSWORD")

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
        sslmode="require"   # important for Supabase
    )
    print("✅ Connection successful!")
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)
