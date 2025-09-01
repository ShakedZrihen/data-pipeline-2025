from __future__ import annotations

import os
import json
import logging
import ssl
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional, Iterable, Dict

import pg8000

try:
    import boto3
except Exception:
    boto3 = None

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("consumer")

# -------------------------
# Normalization & forwarding helpers
# -------------------------
def _normalize_items(items):
    """Map incoming item fields from various producers into the canonical shape this consumer expects."""
    out = []
    for it in items or []:
        # Coerce from JSON strings if needed
        if isinstance(it, str):
            try:
                it = json.loads(it)
            except Exception:
                continue
        if not isinstance(it, dict):
            continue

        prod_name = it.get("product_name") or it.get("name") or it.get("product")
        brand = it.get("brand_name") or it.get("brand")
        if not brand:
            meta = it.get("meta") or {}
            if isinstance(meta, dict):
                brand = meta.get("manufacturer") or meta.get("brand")
        barcode = it.get("barcode") or it.get("gtin") or it.get("sku") or it.get("product_code")

        # price fields
        price = it.get("price") or it.get("current_price") or it.get("amount")
        discount_price = it.get("discount_price") or it.get("sale_price") or it.get("original_price")

        # coerce numeric
        def _to_num(v):
            if v is None:
                return None
            try:
                if isinstance(v, (int, float)):
                    return float(v)
                s = str(v).strip().replace(",", "")
                if s == "":
                    return None
                return float(s)
            except Exception:
                return None

        price = _to_num(price)
        discount_price = _to_num(discount_price)

        # Map date/timestamp to canonical 'ts'
        ts = it.get('ts') or it.get('date') or it.get('timestamp')

        # Strip Hebrew sale prefix '*מבצע* ' and infer discount_price if missing
        if isinstance(prod_name, str):
            s = prod_name.strip()
            if s.startswith("*מבצע*"):
                s = s.replace("*מבצע*", "", 1).strip()
                if discount_price is None and price is not None:
                    discount_price = price
            prod_name = s

        if not prod_name or price is None:
            # Skip incomplete
            continue

        out.append({
            "product_name": prod_name,
            "brand_name": brand,
            "barcode": barcode,
            "ts": ts,
            "price": price,
            "discount_price": discount_price,
            # pass-through any useful fields
            "unit": it.get("unit"),
        })
    return out


def _format_output_payload(provider, branch, typ, timestamp, normalized_items):
    """Format the outward JSON the producer/consumer ecosystem expects (lightweight items)."""
    items = []
    for it in normalized_items:
        items.append({
            "product": it.get("product_name"),
            "price": it.get("price"),
            "unit": it.get("unit") or os.getenv("DEFAULT_UNIT") or "unit"
        })
    return {
        "provider": provider,
        "branch": branch,
        "type": typ or "pricesFull",
        "timestamp": timestamp,
        "items": items,
    }


def _maybe_forward(payload):
    """Optionally forward the normalized payload to SQS or RabbitMQ based on env config."""
    mode = (os.getenv("FORWARD_OUTPUT") or "").lower().strip()
    if not mode:
        return

    try:
        if mode == "sqs":
            import boto3  # available in Lambda runtime
            queue_url = os.getenv("SQS_QUEUE_URL")
            if not queue_url:
                logger.warning("FORWARD_OUTPUT=sqs but SQS_QUEUE_URL not set; skipping forward")
                return
            sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION"))
            sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload, ensure_ascii=False))
            logger.info("Forwarded payload to SQS")
        elif mode == "rabbitmq":
            try:
                import pika  # may not be present in the deployment package
            except Exception:
                logger.warning("FORWARD_OUTPUT=rabbitmq but pika is not installed; skipping forward")
                return
            url = os.getenv("RABBITMQ_URL")
            queue = os.getenv("RABBITMQ_QUEUE") or "prices"
            if not url:
                logger.warning("FORWARD_OUTPUT=rabbitmq but RABBITMQ_URL not set; skipping forward")
                return
            params = pika.URLParameters(url)
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.queue_declare(queue=queue, durable=True)
            ch.basic_publish(exchange="", routing_key=queue, body=json.dumps(payload, ensure_ascii=False))
            conn.close()
            logger.info("Forwarded payload to RabbitMQ")
        else:
            logger.warning("Unknown FORWARD_OUTPUT=%s; skipping forward", mode)
    except Exception:
        logger.exception("Failed to forward payload")


def _record_last_run(provider, branch, typ, ts_iso, processed):
    """Store the last run time per provider+branch+type in DynamoDB or MongoDB if configured."""
    store = (os.getenv("LAST_RUN_STORE") or "").lower().strip()
    if not store:
        return

    key = f"{provider}#{branch}#{typ}"
    record = {
        "pk": key,
        "provider": provider,
        "branch": branch,
        "type": typ,
        "last_run_at": ts_iso,
        "last_processed": int(processed),
        "updated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }

    try:
        if store == "dynamodb":
            import boto3  # present in Lambda
            table_name = os.getenv("DDB_TABLE")
            if not table_name:
                logger.warning("LAST_RUN_STORE=dynamodb but DDB_TABLE not set; skipping store")
                return
            ddb = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION"))
            table = ddb.Table(table_name)
            table.update_item(
                Key={"pk": key},
                UpdateExpression=(
                    "SET provider=:p, branch=:b, #t=:t, last_run_at=:l, "
                    "last_processed=:c, updated_at=:u"
                ),
                ExpressionAttributeNames={"#t": "type"},
                ExpressionAttributeValues={
                    ":p": provider, ":b": branch, ":t": typ,
                    ":l": ts_iso, ":c": int(processed), ":u": record["updated_at"]
                }
            )
            logger.info("Recorded last run in DynamoDB for %s", key)
        elif store == "mongodb":
            try:
                from pymongo import MongoClient  # only if packaged
            except Exception:
                logger.warning("LAST_RUN_STORE=mongodb but pymongo not installed; skipping store")
                return
            mongo_uri = os.getenv("MONGO_URI")
            mongo_db = os.getenv("MONGO_DB") or "prices"
            mongo_coll = os.getenv("MONGO_COLLECTION") or "runs"
            if not mongo_uri:
                logger.warning("LAST_RUN_STORE=mongodb but MONGO_URI not set; skipping store")
                return
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
            coll = client[mongo_db][mongo_coll]
            coll.update_one({"pk": key}, {"$set": record}, upsert=True)
            logger.info("Recorded last run in MongoDB for %s", key)
        else:
            logger.warning("Unknown LAST_RUN_STORE=%s; skipping store", store)
    except Exception:
        logger.exception("Failed to record last run")


# ---------- DB Layer ----------
@dataclass(frozen=True)
class Tables:
    schema: str
    products: str
    branches: str
    prices: str


def _parse_db_url(url: str) -> Dict[str, Any]:
    """Parse postgresql://user:pass@host:port/dbname into parts for pg8000."""
    u = urllib.parse.urlparse(url)
    return {
        "host": u.hostname,
        "port": u.port or 5432,
        "database": (u.path or "/").lstrip("/"),
        "user": urllib.parse.unquote(u.username or ""),
        "password": urllib.parse.unquote(u.password or ""),
    }


class DB:
    def __init__(self, dsn: str, tables: Tables):
        self.tables = tables
        self._dsn = dsn
        self._conn: Any | None = None

    def _exec(self, sql: str, params=None, fetch: str = "none"):
        """Execute a statement safely with consistent cursor usage and auto-reconnect.
        fetch: "none" | "one" | "all"
        """
        conn = self._conn_open()
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
            if fetch == "one":
                try:
                    return cur.fetchone()
                except Exception:
                    return None
            elif fetch == "all":
                try:
                    return cur.fetchall()
                except Exception:
                    return []
            else:
                return None
        except Exception:
            # Reset the connection on protocol/prepared-statement errors
            try:
                cur.close()
            except Exception:
                pass
            try:
                if self._conn:
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                    self._conn = None
            finally:
                pass
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def _conn_open(self):
        if self._conn is None:
            cfg = _parse_db_url(self._dsn)

            # Build TLS context. Prefer explicit CA from env PGSSLROOTCERT.
            # Fallback to bundled file at ./certs/supabase.crt if present.
            cert_path = os.getenv("PGSSLROOTCERT")
            if not cert_path:
                cert_path = os.path.join(os.path.dirname(__file__), "certs", "supabase.crt")

            ctx = ssl.create_default_context()
            if os.path.exists(cert_path):
                try:
                    ctx.load_verify_locations(cafile=cert_path)
                    logger.info("Loaded CA from %s", cert_path)
                except Exception:
                    logger.exception("Failed to load CA at %s; using default trust store", cert_path)
            else:
                logger.warning("CA not found at %s; using default trust store", cert_path)

            self._conn = pg8000.connect(
                user=cfg["user"],
                password=cfg["password"],
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                ssl_context=ctx,   # important for TLS verification
                timeout=5,
            )
            self._conn.autocommit = True
        return self._conn

    def close(self):
        try:
            if self._conn:
                self._conn.close()
        except Exception:
            pass

    # ---------- products ----------
    def upsert_product(self, name: str, brand: Optional[str], barcode: Optional[str]) -> int:
        sch, tp = self.tables.schema, self.tables.products
        # If we have a barcode, use it as the idempotent key
        if barcode:
            row = self._exec(
                f"""
                INSERT INTO {sch}.{tp} (barcode, product_name, brand_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (barcode) DO UPDATE
                SET product_name = COALESCE(EXCLUDED.product_name, {tp}.product_name),
                    brand_name   = COALESCE(EXCLUDED.brand_name,   {tp}.brand_name)
                RETURNING product_id;
                """,
                (barcode, name, brand),
                fetch="one",
            )
            if row:
                return int(row[0])

            # Fallback (older PG versions): fetch by barcode
            row = self._exec(
                f"SELECT product_id FROM {sch}.{tp} WHERE barcode = %s",
                (barcode,),
                fetch="one",
            )
            if row:
                return int(row[0])

        # No barcode or not found: try by (name, brand) with NULL-safe equality
        row = self._exec(
            f"""
            SELECT product_id FROM {sch}.{tp}
             WHERE product_name = %s
               AND brand_name IS NOT DISTINCT FROM %s
            """,
            (name, brand),
            fetch="one",
        )
        if row:
            return int(row[0])

        # Insert without barcode (may be NULL); keep parameter count fixed
        row = self._exec(
            f"""
            INSERT INTO {sch}.{tp} (barcode, product_name, brand_name)
            VALUES (%s, %s, %s)
            RETURNING product_id;
            """,
            (barcode, name, brand),
            fetch="one",
        )
        return int(row[0])

    def get_or_create_branch(self, name: str, address: Optional[str], city: Optional[str]) -> int:
        sch, tb = self.tables.schema, self.tables.branches

        # Find by name + NULL-safe equality on address & city (consistent 3 params)
        row = self._exec(
            f"""
            SELECT branch_id FROM {sch}.{tb}
             WHERE name = %s
               AND address IS NOT DISTINCT FROM %s
               AND city    IS NOT DISTINCT FROM %s
            """,
            (name, address, city),
            fetch="one",
        )
        if row:
            return int(row[0])

        # Insert
        row = self._exec(
            f"""
            INSERT INTO {sch}.{tb} (name, address, city)
            VALUES (%s, %s, %s)
            RETURNING branch_id;
            """,
            (name, address, city),
            fetch="one",
        )
        return int(row[0])

    def insert_price(
        self,
        product_id: int,
        branch_id: int,
        price: Decimal,
        discount_price: Optional[Decimal],
        ts: Any,
    ) -> None:
        sch, tb = self.tables.schema, self.tables.prices

        # Normalize timestamp to a timezone-aware datetime (UTC)
        ts_val = ts
        if isinstance(ts_val, str):
            try:
                if ts_val.endswith("Z"):
                    ts_val = ts_val.replace("Z", "+00:00")
                ts_val = datetime.fromisoformat(ts_val)
            except Exception:
                ts_val = datetime.now(timezone.utc)
        elif isinstance(ts_val, (int, float)):
            ts_val = datetime.fromtimestamp(
                float(ts_val) / (1000.0 if ts_val > 1e12 else 1.0),
                tz=timezone.utc,
            )
        elif isinstance(ts_val, datetime):
            if ts_val.tzinfo is None:
                ts_val = ts_val.replace(tzinfo=timezone.utc)
        else:
            ts_val = datetime.now(timezone.utc)

        self._exec(
            f"""
            INSERT INTO {sch}.{tb} (product_id, branch_id, price, discount_price, ts)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (product_id, branch_id, ts) DO NOTHING
            """,
            (product_id, branch_id, price, discount_price, ts_val),
            fetch="none",
        )

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
            ts_item = it.get("ts") or default_ts
            if isinstance(ts_item, str):
                try:
                    ts_item = datetime.fromisoformat(ts_item.replace("Z", "+00:00"))
                except Exception:
                    ts_item = default_ts
            if getattr(ts_item, "tzinfo", None) is None:
                ts_item = ts_item.replace(tzinfo=timezone.utc)

            price = it.get("price")
            price = Decimal(str(price)) if price is not None else Decimal("0")
            dprice = it.get("discount_price")
            dprice = Decimal(str(dprice)) if dprice is not None else None

            self.insert_price(
                product_id=pid,
                branch_id=branch_id,
                price=price,
                discount_price=dprice,
                ts=ts_item,
            )
            count += 1
        return count


# ---------- Helpers ----------
def _get_env(name: str, default=None):
    return os.getenv(name, default)


def _coerce_to_obj(value):
    """Turn strings/bytes/lists/dicts into a dict structure we can consume."""
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8")
        except Exception:
            return {}
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return {}
    if isinstance(value, list):
        return {"items": value}
    if isinstance(value, dict):
        return value
    return {}


def _load_items_from_s3(bucket: str, key: str):
    if boto3 is None:
        raise RuntimeError("boto3 unavailable")
    from urllib.parse import unquote_plus, unquote

    s3 = boto3.client("s3")
    candidates = [key, unquote_plus(key), unquote(key)]
    for k in candidates:
        try:
            obj = s3.get_object(Bucket=bucket, Key=k)
            body = obj["Body"].read()
            text = body.decode("utf-8")
            try:
                return _coerce_to_obj(json.loads(text))
            except Exception:
                items = []
                for line in text.splitlines():
                    if not line.strip():
                        continue
                    items.append(json.loads(line))
                return {"items": items}
        except Exception:
            continue
    raise FileNotFoundError(f"Could not fetch {bucket}/{key}")



def _extract_items_and_branch(payload: Dict[str, Any] | None):
    p = _coerce_to_obj(payload)
    items = p.get("items") or []
    if isinstance(items, dict):
        items = [items]
    elif not isinstance(items, list):
        items = []

    # Branch may be a string (e.g., "branch_1") or an object
    b = p.get("branch")
    b_is_obj = isinstance(b, dict)
    bname = p.get("branch_name") or (b.get("name") if b_is_obj else (b if isinstance(b, str) else None)) or "unknown"
    baddr = p.get("branch_address") or (b.get("address") if b_is_obj else None)
    bcity = p.get("branch_city") or (b.get("city") if b_is_obj else None)
    return items, bname, baddr, bcity


# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    dsn = _get_env("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL not set")

    tables = Tables(
        schema=_get_env("DB_SCHEMA", "public"),
        products=_get_env("TABLE_PRODUCTS", "products"),
        branches=_get_env("TABLE_BRANCHES", "branches"),
        prices=_get_env("TABLE_PRICES", "prices"),
    )
    db = DB(dsn, tables)

    processed, errors = 0, 0
    batch_failures = []

    records = event.get("Records") if isinstance(event, dict) else [event]
    if not isinstance(records, list):
        records = [records]

    for rec in records:
        try:
            msg = _coerce_to_obj(rec.get("body") if isinstance(rec, dict) else rec)
            if "items_s3_key" in msg:
                bucket = msg.get("items_s3_bucket") or os.getenv("OUTPUT_BUCKET")
                payload = _load_items_from_s3(bucket, msg["items_s3_key"])
            else:
                payload = msg
            items, bname, baddr, bcity = _extract_items_and_branch(payload)
            # Normalize item fields for ingestion
            items = _normalize_items(items)
            # Build an outward payload (optional forward)
            _provider = (payload.get('provider') if isinstance(payload, dict) else None) or os.getenv('PROVIDER') or 'unknown'
            _type = (payload.get('type') if isinstance(payload, dict) else None) or os.getenv('OUTPUT_TYPE') or 'pricesFull'
            _ts = (payload.get('timestamp') if isinstance(payload, dict) else None) or datetime.datetime.utcnow().isoformat(timespec='seconds') + 'Z'
            out_payload = _format_output_payload(_provider, bname or '', _type, _ts, items)
            _maybe_forward(out_payload)
            # Skip manifests or control messages politely
            if isinstance(payload, dict) and payload.get('kind') == 'manifest':
                logger.info("Skipping manifest message for group_id=%s", payload.get('group_id'))
                continue
            if not items:
                logger.info("No items in message; skipping (kind=%s)", (payload.get('kind') if isinstance(payload, dict) else None))
                continue
            cnt = db.batch_ingest(items, branch_name=bname, branch_address=baddr, branch_city=bcity)
            processed += cnt
            # Record last run (optional)
            _record_last_run(_provider, bname or '', _type, _ts, cnt)
        except Exception:
            logger.exception("Failed to ingest batch")
            errors += 1
            if isinstance(rec, dict) and "messageId" in rec:
                batch_failures.append({"itemIdentifier": rec["messageId"]})

    result = {"status": "ok", "processed": processed, "errors": errors}
    logger.info(json.dumps(result))
    db.close()

    if isinstance(event, dict) and isinstance(event.get("Records"), list):
        return {"batchItemFailures": batch_failures}
    return result
