
import os
from datetime import datetime, timezone
from typing import Optional, Tuple, Any, Dict
import psycopg2
from dotenv import load_dotenv

load_dotenv()

class DB:
    """
    Thin convenience wrapper around psycopg2 for your current-only price pipeline.
    Opens and closes a new connection per call (simple & safe for serverless / short-lived workers).
    """

    def __init__(self, database_url: Optional[str] = None) -> None:
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise RuntimeError("Missing DATABASE_URL in environment")

    # ---------- low-level helpers ----------

    def _get_conn(self):
        # Supabase requires SSL
        return psycopg2.connect(self.database_url, sslmode="require")

    def _exec_one(self, sql: str, params: Tuple[Any, ...]) -> Optional[Tuple]:
        conn = self._get_conn()
        conn.autocommit = True
        try:
            with conn, conn.cursor() as cur:
                cur.execute(sql, params)
                try:
                    return cur.fetchone()
                except psycopg2.ProgrammingError:
                    return None
        finally:
            conn.close()

    # ---------- upserts / inserts ----------

    def ensure_supermarket(self, name: str) -> int:
        row = self._exec_one(
            """
            INSERT INTO supermarkets(name)
            VALUES (%s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id;
            """,
            (name,),
        )
        if not row:
            raise RuntimeError(f"ensure_supermarket failed for name={name!r}")
        return int(row[0])

    def ensure_branch(self, supermarket_id: int, branch_code: str, branch_name: Optional[str] = None) -> int:
        row = self._exec_one(
            """
            INSERT INTO branches (supermarket_id, branch_code, branch_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (supermarket_id, branch_code)
            DO UPDATE SET branch_name = COALESCE(EXCLUDED.branch_name, branches.branch_name)
            RETURNING id;
            """,
            (supermarket_id, branch_code, branch_name),
        )
        if not row:
            raise RuntimeError(f"ensure_branch failed for name={branch_name!r}")
        return int(row[0])

    def upsert_provider_product(
        self,
        supermarket_id: int,
        barcode: Optional[str],
        name: str,
        unit_of_measure: Optional[str] = None,
        brand: Optional[str] = None,
        quantity: Optional[float] = None,
    ) -> int:
        """
        Upsert into provider_products, scoped to a supermarket.
        Uniqueness key: (supermarket_id, barcode). Empty string -> NULL.
        """
        barcode_norm = (barcode or "").strip() or None

        row = self._exec_one(
            """
            INSERT INTO provider_products
                (supermarket_id, barcode, name, unit_of_measure, brand, quantity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (supermarket_id, barcode)
            DO UPDATE SET
                name = EXCLUDED.name,
                unit_of_measure = COALESCE(EXCLUDED.unit_of_measure, provider_products.unit_of_measure),
                brand = COALESCE(EXCLUDED.brand, provider_products.brand),
                quantity = COALESCE(EXCLUDED.quantity, provider_products.quantity)
            RETURNING id;
            """,
            (supermarket_id, barcode_norm, name, unit_of_measure, brand, quantity),
        )
        if not row:
            raise RuntimeError(f"upsert_provider_product failed for name={barcode!r}")
        return int(row[0])

    def insert_or_update_current_price(
        self,
        provider_product_id: int,
        branch_id: int,
        price_type: str,               # 'regular' or 'promo'
        price: float,
        effective_at: datetime,
        source_file_type: Optional[str] = None,
    ) -> None:
        """
        Maintain only the current/latest price per (product, branch, type).
        Overwrites existing row for same PK (provider_product_id, branch_id, price_type).
        """
        self._exec_one(
            """
            INSERT INTO current_prices
              (provider_product_id, branch_id, price_type, price, effective_at, source_file_type)
            VALUES
              (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (provider_product_id, branch_id, price_type)
            DO UPDATE SET
              price            = EXCLUDED.price,
              effective_at     = EXCLUDED.effective_at,
              source_file_type = EXCLUDED.source_file_type,
              updated_at       = now();
            """,
            (provider_product_id, branch_id, price_type, price, effective_at, source_file_type),
        )

    # ---------- queries / utilities ----------

    @staticmethod
    def parse_feed_timestamp(ts: str) -> datetime:
        """
        Accepts '2025-08-23T16:49:59Z' or with offset. Returns aware UTC datetime.
        """
        if ts.endswith("Z"):
            ts = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def get_current_price(
        self,
        supermarket_name: str,
        branch_code: str,
        barcode: str,
        price_type: str = "regular",
    ) -> Optional[Dict[str, Any]]:
        """
        Convenience lookup: current price by (provider name, branch code, product barcode, price_type).
        Returns dict or None if not found.
        """
        conn = self._get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT cp.price, cp.effective_at, pp.name, pp.unit_of_measure
                    FROM current_prices cp
                    JOIN provider_products pp ON pp.id = cp.provider_product_id
                    JOIN branches b           ON b.id  = cp.branch_id
                    JOIN supermarkets s       ON s.id  = pp.supermarket_id
                    WHERE s.name = %s
                      AND b.branch_code = %s
                      AND pp.barcode = %s
                      AND cp.price_type = %s
                    """,
                    (supermarket_name, branch_code, barcode, price_type),
                )
                row = cur.fetchone()
                if not row:
                    return None
                price, effective_at, name, uom = row
                return {
                    "price": float(price),
                    "effective_at": effective_at,
                    "name": name,
                    "unit_of_measure": uom,
                }
        finally:
            conn.close()
