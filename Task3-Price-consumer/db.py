import os
import ssl
from typing import List, Dict, Any
import pg8000.dbapi as pg
from dotenv import load_dotenv
load_dotenv()


def _env(name: str, default=None):
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def get_conn():
    host = _env("DB_HOST")
    port = int(os.getenv("DB_PORT", "5432"))
    db = _env("DB_NAME")
    user = _env("DB_USER")
    pwd = _env("DB_PASSWORD")
    sslmode = os.getenv("DB_SSLMODE", "").lower()

    kwargs = {}
    if sslmode == "require":
        kwargs["ssl_context"] = ssl.create_default_context()

    return pg.connect(user=user, password=pwd, host=host, port=port, database=db, **kwargs)


def run_migration_file(conn, sql_path: str):
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    cur = conn.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()


def upsert_rows(conn, rows: List[Dict[str, Any]]) -> int:

    if not rows:
        return 0

    cols = ["provider", "branch", "doc_type", "ts", "product", "unit", "price", "src_key", "etag"]
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"""
        INSERT INTO price_items ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (provider, branch, doc_type, ts, product)
        DO UPDATE SET
          unit       = EXCLUDED.unit,
          price      = EXCLUDED.price,
          src_key    = EXCLUDED.src_key,
          etag       = EXCLUDED.etag,
          updated_at = NOW();
    """
    params = [tuple(r.get(k) for k in cols) for r in rows]

    cur = conn.cursor()
    try:
        cur.executemany(sql, params)
    finally:
        cur.close()
    return len(params)
