# consumer/db.py  -- pg8000 (pure Python) version
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Iterable
from urllib.parse import urlparse, unquote

import pg8000.dbapi as pg

@dataclass(frozen=True)
class Tables:
    schema: str
    products: str
    branches: str
    prices: str

def _parse_dsn(dsn: str):
    """
    Parse postgres URL like:
      postgresql://user:pass@host:5432/dbname?sslmode=require
    into pg8000.connect kwargs.
    """
    u = urlparse(dsn)
    if u.scheme not in ("postgres", "postgresql"):
        raise ValueError("SUPABASE_DB_URL must start with postgresql://")
    user = unquote(u.username or "")
    password = unquote(u.password or "")
    host = u.hostname or "localhost"
    port = int(u.port or 5432)
    database = (u.path or "/").lstrip("/") or "postgres"
    # Supabase requires SSL; if sslmode present we still just set ssl=True
    return dict(user=user, password=password, host=host, port=port, database=database, ssl=True)

class DB:
    def __init__(self, dsn: str, pool_size: int, tables: Tables):
        # pool_size is ignored in this simple version; Lambda reuses the same
        # process so keeping one connection per execution environment is fine.
        self.tables = tables
        self._conn_kwargs = _parse_dsn(dsn)
        self._conn: pg.Connection | None = None

    def _conn_open(self) -> pg.Connection:
        if self._conn is None:
            self._conn = pg.connect(**self._conn_kwargs)
            self._conn.autocommit = True
        return self._conn

    # ---------- products ----------
    def upsert_product(self, name: str, brand: Optional[str], barcode: Optional[str]) -> int:
        sch, tp = self.tables.schema, self.tables.products
        conn = self._conn_open()
        with conn.cursor() as cur:
            if barcode:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.{tp} (barcode, product_name, brand_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (barcode) DO UPDATE
                      SET product_name = EXCLUDED.product_name,
                          brand_name   = COALESCE(EXCLUDED.brand_name, {tp}.brand_name)
                    RETURNING product_id;
                    """,
                    (barcode, name, brand),
                )
                return int(cur.fetchone()[0])

            # fallback by (name, brand) when barcode is missing
            cur.execute(
                f"""
                SELECT product_id
                  FROM {sch}.{tp}
                 WHERE product_name = %s
                   AND (brand_name IS NOT DISTINCT FROM %s)
                 LIMIT 1;
                """,
                (name, brand),
            )
            row = cur.fetchone()
            if row:
                return int(row[0])
            cur.execute(
                f"INSERT INTO {sch}.{tp} (product_name, brand_name) VALUES (%s, %s) RETURNING product_id;",
                (name, brand),
            )
            return int(cur.fetchone()[0])

    # ---------- branches ----------
    def get_or_create_branch(self, name: str, address: Optional[str], city: Optional[str]) -> int:
        sch, tb = self.tables.schema, self.tables.branches
        conn = self._conn_open()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT branch_id FROM {sch}.{tb}
                 WHERE name=%s
                   AND (address IS NOT DISTINCT FROM %s)
                   AND (city    IS NOT DISTINCT FROM %s)
                 LIMIT 1;
                """,
                (name, address, city),
            )
            row = cur.fetchone()
            if row:
                return int(row[0])
            cur.execute(
                f"INSERT INTO {sch}.{tb} (name, address, city) VALUES (%s, %s, %s) RETURNING branch_id;",
                (name, address, city),
            )
            return int(cur.fetchone()[0])

    # ---------- prices ----------
    def insert_price(
        self,
        *,
        product_id: int,
        branch_id: int,
        price: Decimal,
        discount_price: Optional[Decimal],
        ts: datetime,
    ) -> None:
        sch, tp = self.tables.schema, self.tables.prices
        conn = self._conn_open()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {sch}.{tp} (product_id, branch_id, price, discount_price, ts)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (product_id, branch_id, ts) DO NOTHING;
                """,
                (product_id, branch_id, price, discount_price, ts),
            )

    # ---------- batch helper ----------
    def batch_ingest(
        self,
        items: Iterable[dict],
        *,
        branch_name: str,
        branch_address: Optional[str],
        branch_city: Optional[str],
        default_ts: Optional[datetime] = None,
    ) -> int:
        if default_ts is None:
            default_ts = datetime.now(timezone.utc)
        branch_id = self.get_or_create_branch(branch_name, branch_address, branch_city)

        count = 0
        for it in items:
            pid = self.upsert_product(
                name=it["product_name"],
                brand=it.get("brand_name"),
                barcode=it.get("barcode"),
            )
            ts = it.get("ts") or default_ts
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except Exception:
                    ts = default_ts
            if getattr(ts, "tzinfo", None) is None:
                ts = ts.replace(tzinfo=timezone.utc)

            price = it.get("price")
            price = Decimal(str(price)) if price is not None else Decimal("0")
            dprice = it.get("discount_price")
            dprice = Decimal(str(dprice)) if dprice is not None else None

            self.insert_price(
                product_id=pid,
                branch_id=branch_id,
                price=price,
                discount_price=dprice,
                ts=ts,
            )
            count += 1
        return count

    def close(self) -> None:
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass
