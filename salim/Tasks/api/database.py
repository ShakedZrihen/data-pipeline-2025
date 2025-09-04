
import os
from pathlib import Path
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

uri = os.getenv("DATABASE_URL")
if not uri:
    raise RuntimeError("Missing DATABASE_URL in environment")

conn = psycopg2.connect(uri, cursor_factory=psycopg2.extras.RealDictCursor)