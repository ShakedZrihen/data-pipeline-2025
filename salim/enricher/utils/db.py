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
        WHERE conrelid = 'items'::regclass
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
        WHERE conrelid = 'items'::regclass AND contype = 'p'
    """)
    row = cur.fetchone()
    if row:
        cur.execute(f'ALTER TABLE items DROP CONSTRAINT "{row[0]}"')

def _dedupe_items_on_store_code(cur):
    # Keep one arbitrary row per (store_id, code) pair
    cur.execute("""
        WITH d AS (
          SELECT store_id, code
          FROM items
          GROUP BY store_id, code
          HAVING COUNT(*) > 1
        )
        DELETE FROM items a
        USING items b, d
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

            # 3) items (create if missing; otherwise migrate)
            if not _table_exists(cur, "public.items"):
                cur.execute("""
                    CREATE TABLE items(
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
                    ALTER TABLE items
                      ADD COLUMN IF NOT EXISTS last_price_ts TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00+00',
                      ADD COLUMN IF NOT EXISTS last_promo_ts TIMESTAMPTZ DEFAULT '1970-01-01 00:00:00+00'
                """)
                # flip PK to (store_id, code) if needed
                if not _items_pk_is_store_code(cur):
                    _dedupe_items_on_store_code(cur)   # avoid PK violation if dupes exist
                    _drop_current_pk(cur)
                    cur.execute("ALTER TABLE items ADD CONSTRAINT items_current_pkey PRIMARY KEY (store_id, code)")

            # helpful indexes
            cur.execute("CREATE INDEX IF NOT EXISTS items_current_chain_store ON items(chain_id, store_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS items_current_code        ON items(code)")
            cur.execute("CREATE INDEX IF NOT EXISTS items_current_promo_end   ON items(promo_end)")


def fetch_chain_id(conn, provider: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT chain_id FROM supermarkets WHERE LOWER(chain_name) = %s",
            (provider,) 
        )
        row = cur.fetchone()
        return row[0] if row else None


def upsert_supermarket(conn, chain_id: str, chain_name: str):
    print("upsert supermarket called with those params: ", locals())
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO supermarkets(chain_id, chain_name)
            VALUES (%s, %s)
            ON CONFLICT (chain_id) DO UPDATE SET chain_name = EXCLUDED.chain_name
        """, (chain_id, chain_name))

def upsert_store(conn, chain_id: str, store_id: str, store_name: str | None = None,
                 address: str | None = None, city: str | None = None):
    print("upsert store called with those params: ", locals())
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO stores(chain_id, store_id, store_name, address, city)
            VALUES (%s, %s, COALESCE(%s,%s), %s, %s)
            ON CONFLICT (chain_id, store_id) DO UPDATE
            SET store_name = COALESCE(EXCLUDED.store_name, stores.store_name),
                address    = COALESCE(EXCLUDED.address,    stores.address),
                city       = COALESCE(EXCLUDED.city,       stores.city)
        """, (chain_id, store_id, store_name, store_id, address, city))

def fetch_existing_items(conn, chain_id: str, store_id: str, codes: List[str]) -> Dict[str, Dict[str, Any]]:
    if not codes:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT chain_id, store_id, code, name, brand, unit, qty, unit_price,
                   regular_price, promo_price, promo_start, promo_end,
                   last_price_ts, last_promo_ts
            FROM items
            WHERE store_id = %s AND chain_id = %s AND code = ANY(%s)
        """, (store_id, chain_id, codes))
        for row in cur.fetchall():
            out[row["code"]] = dict(row)
    return out

def batch_upsert_items(conn, rows: List[Tuple[Any, ...]]):
    if not rows:
        return
    with conn.cursor() as cur:
        if rows:
            print("First item to upsert:", rows[0])
        execute_values(cur, """
            INSERT INTO items (
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
            regular_price = COALESCE(EXCLUDED.regular_price, items.regular_price),
            promo_price   = EXCLUDED.promo_price,
            promo_start   = EXCLUDED.promo_start,
            promo_end     = EXCLUDED.promo_end,
            last_price_ts = GREATEST(items.last_price_ts, EXCLUDED.last_price_ts),
            last_promo_ts = GREATEST(items.last_promo_ts, EXCLUDED.last_promo_ts)
        """, rows, page_size=1000)

def clear_expired_promos(conn):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE items
            SET promo_price=NULL, promo_start=NULL, promo_end=NULL
            WHERE promo_end IS NOT NULL AND promo_end < NOW()
        """)
