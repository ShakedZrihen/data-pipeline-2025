# utils/db.py
import os
from contextlib import contextmanager
from typing import Any, Dict, List, Tuple
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/prices_db")

@contextmanager
def get_conn():
    conn = psycopg2.connect(DB_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def _table_exists(cur, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (table,))
    return cur.fetchone()[0] is not None

def _items_pk_is_store_code(cur) -> bool:
    cur.execute("""
        SELECT pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'items_current'::regclass
          AND contype = 'p'
    """)
    row = cur.fetchone()
    if not row:
        return False
    return "PRIMARY KEY (store_id, code)" in row[0]

def _drop_current_pk(cur):
    cur.execute("""
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'items_current'::regclass AND contype = 'p'
    """)
    row = cur.fetchone()
    if row:
        cur.execute(f'ALTER TABLE items_current DROP CONSTRAINT "{row[0]}"')

def _dedupe_items_on_store_code(cur):
    # Keep one arbitrary row per (store_id, code) pair
    cur.execute("""
        WITH d AS (
          SELECT store_id, code
          FROM items_current
          GROUP BY store_id, code
          HAVING COUNT(*) > 1
        )
        DELETE FROM items_current a
        USING items_current b, d
        WHERE a.store_id = d.store_id
          AND a.code     = d.code
          AND a.store_id = b.store_id
          AND a.code     = b.code
          AND a.ctid     < b.ctid;  -- keep the latest physical row
    """)

def ensure_schema() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1) supermarkets
            cur.execute("""
                CREATE TABLE IF NOT EXISTS supermarkets(
                  chain_id   TEXT PRIMARY KEY,
                  chain_name TEXT NOT NULL
                )
            """)

            # 2) stores
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stores(
                  store_id   TEXT PRIMARY KEY,
                  store_name TEXT NOT NULL,
                  address    TEXT,
                  city       TEXT
                )
            """)

            # 3) items_current (create if missing; otherwise migrate)
            if not _table_exists(cur, "public.items_current"):
                cur.execute("""
                    CREATE TABLE items_current(
                      chain_id      TEXT NOT NULL REFERENCES supermarkets(chain_id),
                      store_id      TEXT NOT NULL REFERENCES stores(store_id),
                      code          TEXT NOT NULL,

                      name          TEXT,
                      brand         TEXT,
                      unit          TEXT,
                      qty           NUMERIC(12,3),
                      unit_price    NUMERIC(12,4),

                      regular_price NUMERIC(12,2),

                      promo_price   NUMERIC(12,2),
                      promo_start   TIMESTAMPTZ,
                      promo_end     TIMESTAMPTZ,

                      last_price_ts TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00+00',
                      last_promo_ts TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00+00',

                      PRIMARY KEY (store_id, code)
                    )
                """)
            else:
                # add missing timestamp columns (older schema didn’t have them)
                cur.execute("""
                    ALTER TABLE items_current
                      ADD COLUMN IF NOT EXISTS last_price_ts TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00+00',
                      ADD COLUMN IF NOT EXISTS last_promo_ts TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00+00'
                """)
                # flip PK to (store_id, code) if needed
                if not _items_pk_is_store_code(cur):
                    _dedupe_items_on_store_code(cur)   # avoid PK violation if dupes exist
                    _drop_current_pk(cur)
                    cur.execute("ALTER TABLE items_current ADD CONSTRAINT items_current_pkey PRIMARY KEY (store_id, code)")

            # helpful indexes
            cur.execute("CREATE INDEX IF NOT EXISTS items_current_chain_store ON items_current(chain_id, store_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS items_current_code        ON items_current(code)")
            cur.execute("CREATE INDEX IF NOT EXISTS items_current_promo_end   ON items_current(promo_end)")

def upsert_supermarket(conn, chain_id: str, chain_name: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO supermarkets(chain_id, chain_name)
            VALUES (%s, %s)
            ON CONFLICT (chain_id) DO UPDATE SET chain_name = EXCLUDED.chain_name
        """, (chain_id, chain_name))

def upsert_store(conn, store_id: str, store_name: str | None = None, address: str | None = None, city: str | None = None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stores(store_id, store_name, address, city)
            VALUES (%s, COALESCE(%s,%s), %s, %s)
            ON CONFLICT (store_id) DO UPDATE
            SET store_name = COALESCE(EXCLUDED.store_name, stores.store_name),
                address    = COALESCE(EXCLUDED.address,    stores.address),
                city       = COALESCE(EXCLUDED.city,       stores.city)
        """, (store_id, store_name, store_id, address, city))

def fetch_existing_items(conn, store_id: str, codes: List[str]) -> Dict[str, Dict[str, Any]]:
    if not codes:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Use ANY(%s) with a list is fine in psycopg2 if we adapt to array
        cur.execute("""
            SELECT chain_id, store_id, code, name, brand, unit, qty, unit_price,
                   regular_price, promo_price, promo_start, promo_end,
                   last_price_ts, last_promo_ts
            FROM items_current
            WHERE store_id = %s AND code = ANY(%s)
        """, (store_id, codes))
        for row in cur.fetchall():
            out[row["code"]] = dict(row)
    return out

def batch_upsert_items(conn, rows: List[Tuple[Any, ...]]):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO items_current (
                chain_id, store_id, code,
                name, brand, unit, qty, unit_price,
                regular_price, promo_price, promo_start, promo_end,
                last_price_ts, last_promo_ts
            )
            VALUES %s
            ON CONFLICT (store_id, code) DO UPDATE SET
                name          = EXCLUDED.name,
                brand         = EXCLUDED.brand,
                unit          = EXCLUDED.unit,
                qty           = EXCLUDED.qty,
                unit_price    = EXCLUDED.unit_price,
                regular_price = COALESCE(EXCLUDED.regular_price, items_current.regular_price),
                promo_price   = EXCLUDED.promo_price,
                promo_start   = EXCLUDED.promo_start,
                promo_end     = EXCLUDED.promo_end,
                last_price_ts = GREATEST(items_current.last_price_ts, EXCLUDED.last_price_ts),
                last_promo_ts = GREATEST(items_current.last_promo_ts, EXCLUDED.last_promo_ts)
        """, rows, page_size=1000)

def clear_expired_promos(conn):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE items_current
            SET promo_price=NULL, promo_start=NULL, promo_end=NULL
            WHERE promo_end IS NOT NULL AND promo_end < NOW()
        """)
