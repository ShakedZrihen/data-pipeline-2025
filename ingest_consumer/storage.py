from __future__ import annotations
import os
from typing import Dict, Any, Iterable
from psycopg import connect as _pg_connect
from psycopg.rows import dict_row

def connect(dsn: str | None = None):
    if not dsn:
        dsn = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/pipeline")
    return _pg_connect(dsn, row_factory=dict_row)

def upsert_items(pg, payload: Dict[str, Any]) -> int:
    """
    payload = {
      provider, branch, type, timestamp,
      items: [{product, price, unit}, ...]
    }
    """
    provider = payload["provider"]
    branch   = payload["branch"]
    type_    = payload["type"]
    ts       = payload["timestamp"]

    rows = 0
    with pg.cursor() as cur:
        for it in payload["items"]:
            cur.execute(
                """
                INSERT INTO prices (provider, branch, type, ts, product, price, unit)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (provider, branch, type, ts, product) DO UPDATE
                  SET price = EXCLUDED.price,
                      unit  = EXCLUDED.unit
                """,
                (provider, branch, type_, ts, it["product"], it["price"], it.get("unit"))
            )
            rows += 1
        pg.commit()
    return rows
