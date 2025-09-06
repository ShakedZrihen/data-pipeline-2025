import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn():
    dsn = os.getenv("DATABASE_URL") 
    if not dsn:
        raise RuntimeError("DATABASE_URL env var is missing")
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)
