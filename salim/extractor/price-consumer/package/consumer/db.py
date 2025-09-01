# consumer/db.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Iterable

from psycopg_pool import ConnectionPool
import psycopg
from psycopg.rows import dict_row

@dataclass(frozen=True)
class Tables:
    schema: str
    products: str
    branches: str
    prices: str

class DB:
    def __init__(self, dsn: str, pool_size: int, tables: Tables):
        self.tables = tables
        # autocommit=True keeps usage simple per statement
        self.pool = ConnectionPool(
            conninfo=dsn,
            min_size=1,
            max_size=max(1, pool_size),
            kwargs={"autocommit": True},
        )

    # ---------- products ----------
    def upsert_product(self, name: str, brand: Optional[str], barcode: Optional[str]) -> int:
        sch, tp = self.tables.schema, self.tables.products
        with self.pool.connection() as conn, conn.cursor() as cur:
            if barcode:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.{tp} (barcode, product_name, brand_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (barcode) DO UPDATE
                      SET product_name = EXCLUDED.product_name,
                          brand_name = COALESCE(EXCLUDED.brand_name, {tp}.brand_name)
                    RETURNING product_id;
                    """,
                    (barcode, name, brand),
                )
                return cur.fetchone()[0]
            # fallback by (name, brand) to avoid dup rows where barcode missing
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
                return row[0]
            cur.execute(
                f"""
                INSERT INTO {sch}.{tp} (product_name, brand_name)
                VALUES (%s, %s)
                RETURNING product_id;
                """,
                (name, brand),
            )
            return cur.fetchone()[0]

    # ---------- branches ----------
    def get_or_create_branch(self, name: str, address: Optional[str], city: Optional[str]) -> int:
        sch, tb = self.tables.schema, self.tables.branches
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
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
                return int(row["branch_id"])
            cur.execute(
                f"""
                INSERT INTO {sch}.{tb} (name, address, city)
                VALUES (%s, %s, %s)
                RETURNING branch_id;
                """,
                (name, address, city),
            )
            return int(cur.fetchone()["branch_id"])

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
        with self.pool.connection() as conn, conn.cursor() as cur:
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
        """Insert many normalized items into prices table."""
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

            price = Decimal(str(it["price"])) if it.get("price") is not None else Decimal("0")
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
            self.pool.close()
        except Exception:
            pass
