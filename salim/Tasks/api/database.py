
import os
from pathlib import Path
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

def _conn():
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("POSTGRES_URI not set")
    return psycopg2.connect(dsn, sslmode="require", cursor_factory=psycopg2.extras.RealDictCursor)